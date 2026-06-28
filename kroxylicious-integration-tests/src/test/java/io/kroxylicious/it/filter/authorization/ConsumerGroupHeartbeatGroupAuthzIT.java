/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

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

public class ConsumerGroupHeartbeatGroupAuthzIT extends AuthzIT {

    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            EXISTING_TOPIC_NAME);
    public static final String ALICE_GROUP = "alice-group";
    public static final String BOB_GROUP = "bob-group";

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
                from io.kroxylicious.filter.authorization import GroupResource as Group;
                allow User with name = "alice" to * Group with name = "%s";
                allow User with name = "bob" to READ Group with name = "%s";
                otherwise deny;
                """.formatted(ALICE_GROUP, BOB_GROUP));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, EXISTING_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, EXISTING_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, EXISTING_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
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

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        ConsumerGroupHeartbeatRequestData aliceGroupHeartbeat = createHeartbeatRequest(ALL_TOPIC_NAMES_IN_TEST, ALICE_GROUP);
        ConsumerGroupHeartbeatRequestData bobGroupHeartbeat = createHeartbeatRequest(ALL_TOPIC_NAMES_IN_TEST, BOB_GROUP);
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " alice group heartbeat",
                                new ConsumerGroupHeartbeatEquivalence((short) (int) apiVersion, aliceGroupHeartbeat)),
                        Arguments.argumentSet("api version " + apiVersion + " bob group heartbeat",
                                new ConsumerGroupHeartbeatEquivalence((short) (int) apiVersion, bobGroupHeartbeat))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.CONSUMER_GROUP_HEARTBEAT.oldestVersion(), ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.CONSUMER_GROUP_HEARTBEAT, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.CONSUMER_GROUP_HEARTBEAT, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @NonNull
    private static ConsumerGroupHeartbeatRequestData createHeartbeatRequest(List<String> topics, String groupId) {
        ConsumerGroupHeartbeatRequestData allTopics = new ConsumerGroupHeartbeatRequestData();
        allTopics.setSubscribedTopicNames(topics);
        allTopics.setGroupId(groupId);
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
