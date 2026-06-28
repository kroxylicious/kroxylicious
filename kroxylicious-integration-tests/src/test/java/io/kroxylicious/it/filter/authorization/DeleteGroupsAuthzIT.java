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
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
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

class DeleteGroupsAuthzIT extends AuthzIT {

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
                                AclOperation.DELETE, AclPermissionType.ALLOW)));

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

    class DeleteGroupsEquivalence extends Equivalence<DeleteGroupsRequestData, DeleteGroupsResponseData> {

        private final String group;
        private final String groupPrefix;

        DeleteGroupsEquivalence(
                                short apiVersion,
                                String groupPrefix) {
            super(apiVersion);
            this.groupPrefix = groupPrefix;
            this.group = groupPrefix + "-" + UUID.randomUUID();
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
            return ApiKeys.DELETE_GROUPS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            // join then leave group to drive it to a deletable state
            driver.findCoordinator(CoordinatorType.GROUP, group);
            String groupInstanceId = group + "-instanceId";
            JoinGroupResponseData responseData = driver.joinGroup("consumer", group, groupInstanceId);
            driver.leaveGroup(group, responseData.memberId(), groupInstanceId);
        }

        @Override
        public DeleteGroupsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            DeleteGroupsRequestData deleteGroupsRequestData = new DeleteGroupsRequestData();
            deleteGroupsRequestData.groupsNames().add(group);
            return deleteGroupsRequestData;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, DeleteGroupsResponseData> unproxiedResponsesByUser) {
            assertUserReceivedErrorCodesPerGroup(unproxiedResponsesByUser, EVE, Map.of(group, Errors.GROUP_AUTHORIZATION_FAILED));
            if (groupPrefix.equals(ALICE_GROUP_PREFIX)) {
                assertUserReceivedErrorCodesPerGroup(unproxiedResponsesByUser, ALICE, Map.of(group, Errors.NONE));
                assertUserReceivedErrorCodesPerGroup(unproxiedResponsesByUser, BOB, Map.of(group, Errors.GROUP_AUTHORIZATION_FAILED));
            }
            else if (groupPrefix.equals(BOB_GROUP_PREFIX)) {
                assertUserReceivedErrorCodesPerGroup(unproxiedResponsesByUser, ALICE, Map.of(group, Errors.GROUP_AUTHORIZATION_FAILED));
                assertUserReceivedErrorCodesPerGroup(unproxiedResponsesByUser, BOB, Map.of(group, Errors.NONE));
            }
        }

        private static void assertUserReceivedErrorCodesPerGroup(Map<String, DeleteGroupsResponseData> unproxiedResponsesByUser, String user,
                                                                 Map<String, Errors> expected) {
            Map<String, Errors> groupToError = unproxiedResponsesByUser.get(user).results().stream()
                    .collect(Collectors.toMap(DeleteGroupsResponseData.DeletableGroupResult::groupId, r -> Errors.forCode(r.errorCode())));
            assertThat(groupToError).containsExactlyInAnyOrderEntriesOf(expected);
        }

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        // The tuples
        List<Short> apiVersions = ApiKeys.DELETE_GROUPS.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (!AuthorizationFilter.isApiVersionSupported(ApiKeys.DELETE_GROUPS, apiVersion)) {
                UnsupportedApiVersion<ApiMessage, ApiMessage> apiMessageApiMessageUnsupportedApiVersion = new UnsupportedApiVersion<>(ApiKeys.DELETE_GROUPS, apiVersion);
                result.add(
                        Arguments.of(apiMessageApiMessageUnsupportedApiVersion));
                continue;
            }

            result.add(Arguments.of(new DeleteGroupsEquivalence(apiVersion, ALICE_GROUP_PREFIX)));
            result.add(Arguments.of(new DeleteGroupsEquivalence(apiVersion, BOB_GROUP_PREFIX)));

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
