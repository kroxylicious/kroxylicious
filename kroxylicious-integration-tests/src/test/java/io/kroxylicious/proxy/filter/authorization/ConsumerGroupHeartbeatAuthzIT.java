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
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
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

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.stream.Stream.concat;

public class ConsumerGroupHeartbeatAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TOPIC_NAME = "eve-topic";
    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME,
            EVE_TOPIC_NAME,
            EXISTING_TOPIC_NAME);
    public static final String GROUP_ID = "group";

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException, ExecutionException, InterruptedException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DESCRIBE Topic with name = "%s";
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
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                allowAllOnGroup(ALICE, GROUP_ID),
                allowAllOnGroup(BOB, GROUP_ID),
                allowAllOnGroup(EVE, GROUP_ID));
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

    class ConsumerGroupHeartbeatEquivalence extends Equivalence<ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData> {

        private final ConsumerGroupHeartbeatRequestData requestData;

        ConsumerGroupHeartbeatEquivalence(short apiVersion, ConsumerGroupHeartbeatRequestData requestData) {
            super(apiVersion);
            this.requestData = requestData;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.CONSUMER_GROUP_HEARTBEAT;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public ConsumerGroupHeartbeatRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestData;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
        }

        @Override
        public void assertUnproxiedResponses(Map<String, ConsumerGroupHeartbeatResponseData> unproxiedResponsesByUser) {

        }

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        ConsumerGroupHeartbeatRequestData allTopics = createHeartbeatRequest(ALL_TOPIC_NAMES_IN_TEST);
        ConsumerGroupHeartbeatRequestData aliceTopic = createHeartbeatRequest(List.of(ALICE_TOPIC_NAME));
        ConsumerGroupHeartbeatRequestData bobTopic = createHeartbeatRequest(List.of(BOB_TOPIC_NAME));
        ConsumerGroupHeartbeatRequestData eveTopic = createHeartbeatRequest(List.of(EVE_TOPIC_NAME));

        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " all topics request",
                                new ConsumerGroupHeartbeatEquivalence((short) (int) apiVersion, allTopics)),
                        Arguments.argumentSet("api version " + apiVersion + " alice topic request",
                                new ConsumerGroupHeartbeatEquivalence((short) (int) apiVersion, aliceTopic)),
                        Arguments.argumentSet("api version " + apiVersion + " bob topic request",
                                new ConsumerGroupHeartbeatEquivalence((short) (int) apiVersion, bobTopic)),
                        Arguments.argumentSet("api version " + apiVersion + " eve topic request",
                                new ConsumerGroupHeartbeatEquivalence((short) (int) apiVersion, eveTopic))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.CONSUMER_GROUP_HEARTBEAT.oldestVersion(), ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.CONSUMER_GROUP_HEARTBEAT, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.CONSUMER_GROUP_HEARTBEAT, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @NonNull
    private static ConsumerGroupHeartbeatRequestData createHeartbeatRequest(List<String> topics) {
        ConsumerGroupHeartbeatRequestData allTopics = new ConsumerGroupHeartbeatRequestData();
        allTopics.setSubscribedTopicNames(topics);
        allTopics.setGroupId(GROUP_ID);
        allTopics.setInstanceId("instance");
        allTopics.setMemberId("member");
        return allTopics;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
