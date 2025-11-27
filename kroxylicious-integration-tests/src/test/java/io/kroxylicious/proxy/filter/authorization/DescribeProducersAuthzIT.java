/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.stream.Stream.concat;

class DescribeProducersAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TOPIC_NAME = "eve-topic";
    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME,
            EVE_TOPIC_NAME,
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
                allow User with name = "bob" to READ Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TOPIC_NAME, BOB_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.READ, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    class DescribeProducersEquivalence extends Equivalence<DescribeProducersRequestData, DescribeProducersResponseData> {

        private final DescribeProducersRequestData requestData;

        DescribeProducersEquivalence(short apiVersion, DescribeProducersRequestData requestData) {
            super(apiVersion);
            this.requestData = requestData;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.DESCRIBE_PRODUCERS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public DescribeProducersRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestData;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        private static void clobber(final JsonNode root) {
            if (root.isObject()) {
                clobberUuid((ObjectNode) root, "topicId");
                sortArray((ObjectNode) root, "topics", "name");
                root.values().forEachRemaining(DescribeProducersEquivalence::clobber);
            }
            if (root.isArray()) {
                root.forEach(DescribeProducersEquivalence::clobber);
            }
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
        }

        @Override
        public void assertUnproxiedResponses(Map<String, DescribeProducersResponseData> unproxiedResponsesByUser) {

        }

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        DescribeProducersRequestData allTopics = describeProducersRequest(ALL_TOPIC_NAMES_IN_TEST);
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.DESCRIBE_PRODUCERS),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.DESCRIBE_PRODUCERS))
                .boxed().flatMap(apiVersion -> {
                    Stream<Arguments.ArgumentSet> all = Stream.of(
                            Arguments.argumentSet("api version " + apiVersion + " all topics request",
                                    new DescribeProducersEquivalence((short) (int) apiVersion, allTopics)));
                    Stream<Arguments.ArgumentSet> perTopic = ALL_TOPIC_NAMES_IN_TEST.stream()
                            .map(topic -> Arguments.argumentSet("api version " + apiVersion + " " + topic + " request",
                                    new DescribeProducersEquivalence((short) (int) apiVersion, describeProducersRequest(List.of(topic)))));
                    return Stream.concat(all, perTopic);
                });
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.DESCRIBE_PRODUCERS.oldestVersion(), ApiKeys.DESCRIBE_PRODUCERS.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.DESCRIBE_PRODUCERS, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.DESCRIBE_PRODUCERS, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @NonNull
    private static DescribeProducersRequestData describeProducersRequest(List<String> topics) {
        DescribeProducersRequestData specificTopics = new DescribeProducersRequestData();
        topics.forEach(topicName -> {
            DescribeProducersRequestData.TopicRequest topicRequest = new DescribeProducersRequestData.TopicRequest();
            topicRequest.setName(topicName);
            topicRequest.setPartitionIndexes(List.of(0));
            specificTopics.topics().add(topicRequest);
        });
        return specificTopics;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<DescribeTopicPartitionsRequestData, DescribeTopicPartitionsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
