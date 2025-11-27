/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static org.assertj.core.api.Assertions.assertThat;

class CreateTopicsAuthzIT extends AuthzIT {

    private static final String ALICE_TO_CREATE_TOPIC_NAME = "alice-new-topic";
    private static final String BOB_TO_CREATE_TOPIC_NAME = "bob-new-topic";
    private static final String EVE_TO_CREATE_TOPIC_NAME = "eve-new-topic";
    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TO_CREATE_TOPIC_NAME,
            BOB_TO_CREATE_TOPIC_NAME,
            EVE_TO_CREATE_TOPIC_NAME,
            EXISTING_TOPIC_NAME);

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to CREATE Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TO_CREATE_TOPIC_NAME, BOB_TO_CREATE_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TO_CREATE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TO_CREATE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.CREATE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, List.of(EXISTING_TOPIC_NAME), aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, List.of(EXISTING_TOPIC_NAME), List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    class CreateTopicsEquivalence extends Equivalence<CreateTopicsRequestData, CreateTopicsResponseData> {

        private final RequestTemplate<CreateTopicsRequestData> requestTemplate;

        CreateTopicsEquivalence(
                                short apiVersion,
                                RequestTemplate<CreateTopicsRequestData> requestTemplate) {
            super(apiVersion);
            this.requestTemplate = requestTemplate;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "apiVersion=" + apiVersion() +
                    ", requestTemplate=" + requestTemplate +
                    '}';
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.CREATE_TOPICS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public CreateTopicsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestTemplate.request(user, clusterFixture);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            var topics = sortArray(jsonNodes, "topics", "name");
            for (var topics1 : topics) {
                if (topics1.isObject()) {
                    clobberUuid((ObjectNode) topics1, "topicId");
                }
            }
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            assertThat(topicListing(cluster))
                    .isEqualTo(Set.of(
                            EXISTING_TOPIC_NAME, ALICE_TO_CREATE_TOPIC_NAME, BOB_TO_CREATE_TOPIC_NAME));
        }

        @Override
        public Object observedVisibleSideEffects(BaseClusterFixture cluster) {
            return topicListing(cluster);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, CreateTopicsResponseData> unproxiedResponsesByUser) {
            // TODO
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        // The tuples
        List<Short> apiVersions = ApiKeys.CREATE_TOPICS.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (!AuthorizationFilter.isApiVersionSupported(ApiKeys.CREATE_TOPICS, apiVersion)) {
                UnsupportedApiVersion<ApiMessage, ApiMessage> apiMessageApiMessageUnsupportedApiVersion = new UnsupportedApiVersion<>(ApiKeys.CREATE_TOPICS, apiVersion);
                result.add(
                        Arguments.of(apiMessageApiMessageUnsupportedApiVersion));
                continue;
            }

            result.add(
                    Arguments.of(new CreateTopicsEquivalence(apiVersion, new RequestTemplate<CreateTopicsRequestData>() {

                        List<CreateTopicsRequestData.CreatableTopic> topics = ALL_TOPIC_NAMES_IN_TEST.stream().map(name -> new CreateTopicsRequestData.CreatableTopic()
                                .setName(name)
                                .setNumPartitions(1)
                                .setReplicationFactor((short) 1))
                                .toList();

                        @Override
                        public CreateTopicsRequestData request(String user, BaseClusterFixture clusterFixture) {
                            var data = new CreateTopicsRequestData();
                            data.topics().addAll(duplicateList(topics));
                            return data;
                        }

                        @Override
                        public String toString() {
                            return topics.toString();
                        }
                    })));

        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<CreateTopicsRequestData, CreateTopicsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
