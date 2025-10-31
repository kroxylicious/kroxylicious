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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
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

import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

public class CreatePartitionsAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TOPIC_NAME = "eve-topic";
    private static final String NON_EXISTING_TOPIC_NAME = "non-existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME,
            EVE_TOPIC_NAME,
            NON_EXISTING_TOPIC_NAME);

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @BeforeAll
    void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                version 1;
                import User from io.kroxylicious.proxy.authentication;
                import TopicResource as Topic from io.kroxylicious.filter.authorization;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to ALTER Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TOPIC_NAME, BOB_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALTER, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        var createTopics = List.of(
                ALICE_TOPIC_NAME,
                BOB_TOPIC_NAME,
                EVE_TOPIC_NAME);
        prepCluster(kafkaClusterWithAuthz, createTopics, aclBindings);
        prepCluster(kafkaClusterNoAuthz, createTopics, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    class CreatePartitionsEquivalence extends Equivalence<CreatePartitionsRequestData, CreatePartitionsResponseData> {

        private final RequestTemplate<CreatePartitionsRequestData> requestTemplate;

        CreatePartitionsEquivalence(
                                    short apiVersion,
                                    RequestTemplate<CreatePartitionsRequestData> requestTemplate) {
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
            return ApiKeys.CREATE_PARTITIONS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public CreatePartitionsRequestData requestData(String user, Map<String, Uuid> topicNameToId) {
            return requestTemplate.request(user, topicNameToId);
        }

        @Override
        public ObjectNode convertResponse(CreatePartitionsResponseData response) {
            return (ObjectNode) CreatePartitionsResponseDataJsonConverter.write(response, apiVersion());
        }

        private Map<String, Integer> numPartitions(KafkaCluster cluster) {
            try (var admin = Admin.create(cluster.getKafkaClientConfiguration(SUPER, "Super"))) {
                var topics = admin.describeTopics(ALL_TOPIC_NAMES_IN_TEST).topicNameValues();
                return topics.values().stream().map(
                        value -> value.toCompletionStage().toCompletableFuture())
                        .filter(fut -> {
                            try {
                                fut.join();
                                return true;
                            }
                            catch (CompletionException e) {
                                return false;
                            }
                        }).map(CompletableFuture::join)
                        .collect(Collectors.toMap(TopicDescription::name,
                                td -> td.partitions().size()));
            }
        }

        @Override
        public String clobberResponse(ObjectNode jsonNodes) {
            var topics = sortArray(jsonNodes, "topics", "name");

            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            assertThat(numPartitions(cluster.backingCluster()))
                    .isEqualTo(Map.of(
                            BOB_TOPIC_NAME, 2,
                            ALICE_TOPIC_NAME, 2,
                            EVE_TOPIC_NAME, 1));
        }

        @Override
        public void assertUnproxiedResponses(Map<String, CreatePartitionsResponseData> unproxiedResponsesByUser) {
            // TODO
        }
    }

    List<Arguments> test() {
        // The tuples
        List<Short> apiVersions = ApiKeys.CREATE_PARTITIONS.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            result.add(
                    Arguments.of(new CreatePartitionsEquivalence(apiVersion, (user, topicNameToId) -> {
                        var topic = new CreatePartitionsRequestData.CreatePartitionsTopic()
                                .setName(user + "-topic")
                                .setCount(2)
                                .setAssignments(List.of(
                                        new CreatePartitionsRequestData.CreatePartitionsAssignment().setBrokerIds(List.of(0))));
                        var b = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();
                        b.mustAdd(topic);
                        return new CreatePartitionsRequestData()
                                .setTopics(b)
                                .setTimeoutMs(60_000);
                    })));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void test(VersionSpecificVerification<CreatePartitionsRequestData, CreatePartitionsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
