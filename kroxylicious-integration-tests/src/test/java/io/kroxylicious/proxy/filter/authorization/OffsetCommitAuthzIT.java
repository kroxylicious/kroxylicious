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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
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
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetCommitAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TOPIC_NAME = "eve-topic";
    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME,
            EVE_TOPIC_NAME,
            EXISTING_TOPIC_NAME);

    private static final Map<String, String> GROUP_PER_USER = Map.of(ALICE, "foo-" + ALICE,
            BOB, "foo-" + BOB,
            EVE, "foo-" + EVE);

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
                """.formatted(ALICE_TOPIC_NAME, BOB_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // group permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, "foo-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, "foo-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                // Allow Eve to access the group, so we can test the authorization of the topic
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, "foo-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),

                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.CREATE, AclPermissionType.ALLOW)));

        // ensureInternalTopicsExist(kafkaClusterWithAuthz, "tmpvsdvsv");
        // ensureInternalTopicsExist(kafkaClusterNoAuthz, "tmp");
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

    Map<String, GroupContext> groupContexts = new HashMap<>();

    class OffsetCommitEquivalence extends Equivalence<OffsetCommitRequestData, OffsetCommitResponseData> {

        private final RequestTemplate<OffsetCommitRequestData> requestTemplate;

        OffsetCommitEquivalence(
                                short apiVersion,
                                RequestTemplate<OffsetCommitRequestData> requestTemplate) {
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
            return ApiKeys.OFFSET_COMMIT;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            Map<String, KafkaClient> stringKafkaClientMap = cluster.authenticatedClients(PASSWORDS.keySet());
            stringKafkaClientMap.forEach((username, kafkaClient) -> {
                var groupContext = new GroupContext(username, GROUP_PER_USER.get(username));
                groupContext.doUptoSyncedGroup(cluster, kafkaClient);
                groupContexts.put(cluster.name() + username, groupContext);
            });
        }

        @Override
        public OffsetCommitRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestTemplate.request(user, clusterFixture);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            Object observed = observedVisibleSideEffects(cluster);
            assertThat(observed).isEqualTo(Map.of(
                    ALICE, Map.of(new TopicPartition("alice-topic", 0), 420L),
                    BOB, Map.of(),
                    EVE, Map.of()));
        }

        @Override
        public Object observedVisibleSideEffects(BaseClusterFixture cluster) {
            return GROUP_PER_USER.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> offsets(cluster, entry.getValue())));
        }

        @Override
        public void assertUnproxiedResponses(Map<String, OffsetCommitResponseData> unproxiedResponsesByUser) {
            assertThat(unproxiedResponsesByUser.get(ALICE).topics().stream()
                    .flatMap(t -> t.partitions().stream().map(p -> {
                        return Map.entry(new TopicPartition(t.name(), p.partitionIndex()), Errors.forCode(p.errorCode()));
                    }))
                    .filter(e -> e.getValue() != Errors.NONE)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .as("%s offsets in %s", ALICE, unproxiedResponsesByUser)
                    .isEmpty();

            assertThat(unproxiedResponsesByUser.get(EVE).topics().stream()
                    .flatMap(t -> t.partitions().stream().map(p -> {
                        return Map.entry(new TopicPartition(t.name(), p.partitionIndex()), Errors.forCode(p.errorCode()));
                    }))
                    .filter(e -> e.getValue() != Errors.TOPIC_AUTHORIZATION_FAILED)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .as("%s offsets in %s", EVE, unproxiedResponsesByUser)
                    .isEmpty();
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        // The tuples
        List<Short> apiVersions = ApiKeys.OFFSET_COMMIT.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (!AuthorizationFilter.isApiVersionSupported(ApiKeys.OFFSET_COMMIT, apiVersion)) {
                UnsupportedApiVersion<ApiMessage, ApiMessage> apiMessageApiMessageUnsupportedApiVersion = new UnsupportedApiVersion<>(ApiKeys.OFFSET_COMMIT, apiVersion);
                result.add(
                        Arguments.of(apiMessageApiMessageUnsupportedApiVersion));
                continue;
            }

            result.add(
                    Arguments.of(new OffsetCommitEquivalence(apiVersion, new RequestTemplate<OffsetCommitRequestData>() {

                        @Override
                        public OffsetCommitRequestData request(String user, BaseClusterFixture clusterFixture) {
                            var offsetCommitRequestTopic = new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                    .setName(user + "-topic");
                            offsetCommitRequestTopic
                                    .partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                            .setPartitionIndex(0)
                                            .setCommittedOffset(420)
                                            .setCommittedMetadata("")
                                            .setCommittedLeaderEpoch(1));

                            var data = new OffsetCommitRequestData();
                            String groupId = GROUP_PER_USER.get(user);
                            data.setGroupId(groupId);
                            if (apiVersion >= 7) {
                                data.setGroupInstanceId(groupId + "-" + user);
                            }

                            GroupContext groupContext = Objects.requireNonNull(groupContexts.get(clusterFixture.name() + user));
                            data.setMemberId(Objects.requireNonNull(groupContext.memberId));
                            data.setGenerationIdOrMemberEpoch(groupContext.generation);
                            data.setRetentionTimeMs(123);
                            data.topics().add(offsetCommitRequestTopic);
                            return data;
                        }

                    })));

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

    class GroupContext {

        private final String groupId;
        private final String groupInstanceId;
        private static final String PROTOCOL_TYPE = "consumer";
        private final String username;
        private int generation = 0;
        private String memberId;

        GroupContext(String username, String groupId) {
            this.username = username;
            this.groupId = groupId;
            this.groupInstanceId = groupId + "-" + username;
        }

        public void doUptoSyncedGroup(BaseClusterFixture cluster, KafkaClient kafkaClient) {
            KafkaDriver kafkaDriver = new KafkaDriver(cluster, kafkaClient, username);
            kafkaDriver.findCoordinator(CoordinatorType.GROUP, groupId);
            var joinGroupResponse = kafkaDriver.joinGroup(PROTOCOL_TYPE, groupId, groupInstanceId);
            this.memberId = Objects.requireNonNull(joinGroupResponse.memberId());
            this.generation = joinGroupResponse.generationId();
            kafkaDriver.syncGroup(groupId, groupInstanceId, PROTOCOL_TYPE, generation, joinGroupResponse.memberId());
        }

    }

}
