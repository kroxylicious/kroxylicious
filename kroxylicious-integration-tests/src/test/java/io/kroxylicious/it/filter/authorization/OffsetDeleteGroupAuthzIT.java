/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
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

class OffsetDeleteGroupAuthzIT extends AuthzIT {

    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(EXISTING_TOPIC_NAME);
    private static final String ALICE_GROUP_PREFIX = "alice-group";
    private static final String BOB_GROUP_PREFIX = "bob-group";

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
                allow User with name = "alice" to * Group with name like "%s*";
                allow User with name = "bob" to DELETE Group with name like "%s*";
                allow User with name = "super" to * Group with name like "*";
                otherwise deny;
                """.formatted(ALICE_GROUP_PREFIX, BOB_GROUP_PREFIX));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // group permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DELETE, AclPermissionType.ALLOW)),

                // topic permissions
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

    class OffsetDeleteEquivalence extends Equivalence<OffsetDeleteRequestData, OffsetDeleteResponseData> {

        private final String group;
        private final String groupInstanceId;
        private final String groupPrefix;
        private String memberId;
        private int generation;

        OffsetDeleteEquivalence(
                                short apiVersion,
                                String groupPrefix) {
            super(apiVersion);
            this.groupPrefix = groupPrefix;
            this.group = groupPrefix + "-" + UUID.randomUUID();
            this.groupInstanceId = group + "-instanceId";
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "apiVersion=" + apiVersion() +
                    ", groupPrefix=" + groupPrefix +
                    '}';
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.OFFSET_DELETE;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(CoordinatorType.GROUP, group);
            var joinGroupResponse = driver.joinGroup("consumer", group, groupInstanceId);
            this.memberId = Objects.requireNonNull(joinGroupResponse.memberId());
            this.generation = joinGroupResponse.generationId();
            driver.syncGroup(group, groupInstanceId, "consumer", generation, joinGroupResponse.memberId());
            driver.commitOffsets(group, memberId, groupInstanceId, generation, EXISTING_TOPIC_NAME, Map.of(0, 1));
            driver.leaveGroup(group, memberId, groupInstanceId);
        }

        @Override
        public OffsetDeleteRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            OffsetDeleteRequestData request = new OffsetDeleteRequestData();
            request.setGroupId(group);
            OffsetDeleteRequestData.OffsetDeleteRequestTopic deleteOffsets = new OffsetDeleteRequestData.OffsetDeleteRequestTopic();
            deleteOffsets.setName(EXISTING_TOPIC_NAME);
            OffsetDeleteRequestData.OffsetDeleteRequestPartition deleteRequestPartition = new OffsetDeleteRequestData.OffsetDeleteRequestPartition();
            deleteRequestPartition.setPartitionIndex(0);
            deleteOffsets.partitions().add(deleteRequestPartition);
            request.topics().add(deleteOffsets);
            return request;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            Object observed = observedVisibleSideEffects(cluster);
            // offsets for partition 0 should be removed
            assertThat(observed).isEqualTo(Map.of());
        }

        @Override
        public Object observedVisibleSideEffects(BaseClusterFixture cluster) {
            return offsets(cluster, group);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, OffsetDeleteResponseData> unproxiedResponsesByUser) {
            assertThat(Errors.forCode(unproxiedResponsesByUser.get(EVE).errorCode())).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
            if (groupPrefix.equals(ALICE_GROUP_PREFIX)) {
                assertThat(Errors.forCode(unproxiedResponsesByUser.get(BOB).errorCode())).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
                assertOffsetsDeletedSuccessfully(unproxiedResponsesByUser.get(ALICE));
            }
            else if (groupPrefix.equals(BOB_GROUP_PREFIX)) {
                assertThat(Errors.forCode(unproxiedResponsesByUser.get(ALICE).errorCode())).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
                assertOffsetsDeletedSuccessfully(unproxiedResponsesByUser.get(BOB));
            }
        }

        private static void assertOffsetsDeletedSuccessfully(OffsetDeleteResponseData bobResponse) {
            assertThat(Errors.forCode(bobResponse.errorCode())).isEqualTo(Errors.NONE);
            assertThat(bobResponse.topics()).isNotEmpty().allSatisfy(responseTopic -> {
                assertThat(responseTopic.partitions()).isNotEmpty().allSatisfy(responsePartition -> {
                    assertThat(Errors.forCode(responsePartition.errorCode())).isEqualTo(Errors.NONE);
                });
            });
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        // The tuples
        List<Short> apiVersions = ApiKeys.OFFSET_DELETE.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (!AuthorizationFilter.isApiVersionSupported(ApiKeys.OFFSET_DELETE, apiVersion)) {
                UnsupportedApiVersion<ApiMessage, ApiMessage> apiMessageApiMessageUnsupportedApiVersion = new UnsupportedApiVersion<>(ApiKeys.OFFSET_DELETE, apiVersion);
                result.add(
                        Arguments.of(apiMessageApiMessageUnsupportedApiVersion));
                continue;
            }

            result.add(Arguments.of(new OffsetDeleteEquivalence(apiVersion, ALICE_GROUP_PREFIX)));
            result.add(Arguments.of(new OffsetDeleteEquivalence(apiVersion, BOB_GROUP_PREFIX)));

        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<OffsetCommitRequestData, OffsetCommitResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
