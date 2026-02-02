/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
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

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.deleteTopicsAndAcls;
import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that actors must have write permissions for the tranactionalId that they reference
 * in TxnOffsetCommit RPCs. Note that the actor also needs permissions to read each
 * topic in the request, which is covered in a separate test.
 * <p>
 * Note that to init the transactional producer id the user needs WRITE permissions for the TransactionalId.
 * So to test that TxnOffsetCommit fails if the user 'eve' is not permitted, we use a permitted user
 * 'alice' to drive the system into the expected state before the TxnOffsetCommit, then have 'eve'
 * attempt to commit the offsets.
 */
class TxnOffsetCommitTxnIdAuthzIT extends AuthzIT {

    private Path rulesFile;
    private static final String TOPIC_NAME = "shared-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(TOPIC_NAME);
    private static List<AclBinding> aclBindings;
    private final Map<String, TestState> testStatesPerClusterUser = new HashMap<>();

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                from io.kroxylicious.filter.authorization import TransactionalIdResource as TxnId;
                allow User with name = "alice" to * TxnId with name like "trans-*";
                allow User with name = "bob" to {WRITE,DESCRIBE} TxnId with name like "trans-*";
                allow User with name like "*" to * Topic with name like "*";
                otherwise deny;
                """);

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),

                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, "group-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, "group-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, "group-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),

                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "trans-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "trans-", PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)));

    }

    @BeforeEach
    void prepClusters() {
        try {
            this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
            this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    record TestState(KafkaClient superClient, String transactionalId, String groupId, ProducerIdAndEpoch producerIdAndEpoch, int generationId, String memberId) {}

    List<Arguments> shouldEnforceAccessToTopics() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.TXN_OFFSET_COMMIT),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.TXN_OFFSET_COMMIT))
                .mapToObj(apiVersion -> Arguments.argumentSet("api version " + apiVersion, new TxnOffsetCommitEquivalence((short) apiVersion)));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.OFFSET_FOR_LEADER_EPOCH, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.OFFSET_FOR_LEADER_EPOCH, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class TxnOffsetCommitEquivalence extends Equivalence<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> {

        TxnOffsetCommitEquivalence(short apiVersion) {
            super(apiVersion);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        record TopicPartitionError(String topic, int partition, Errors errors) {}

        @Override
        public void assertUnproxiedResponses(Map<String, TxnOffsetCommitResponseData> unproxiedResponsesByUser) {
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, BOB, new TopicPartitionError(TOPIC_NAME, 0, Errors.NONE));
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, ALICE, new TopicPartitionError(TOPIC_NAME, 0, Errors.NONE));
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, EVE, new TopicPartitionError(TOPIC_NAME, 0, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        }

        private void assertThatUserHasTopicPartitions(Map<String, TxnOffsetCommitResponseData> unproxiedResponsesByUser, String user,
                                                      TopicPartitionError... topicPartitionErrors) {
            TxnOffsetCommitResponseData txnOffsetCommitResponseData = unproxiedResponsesByUser.get(user);
            assertThat(txnOffsetCommitResponseData).isNotNull();
            Set<TopicPartitionError> collect = getTopicPartitionErrors(txnOffsetCommitResponseData);
            assertThat(collect).describedAs("partition errors for user " + user).containsExactlyInAnyOrder(topicPartitionErrors);
        }

        private static Set<TopicPartitionError> getTopicPartitionErrors(TxnOffsetCommitResponseData response) {
            return response.topics().stream()
                    .flatMap(topic -> topic.partitions().stream()
                            .map(partition -> new TopicPartitionError(topic.name(), partition.partitionIndex(), Errors.forCode(partition.errorCode()))))
                    .collect(toSet());
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.TXN_OFFSET_COMMIT;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public TxnOffsetCommitRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            TestState state = Objects.requireNonNull(testStatesPerClusterUser.get(clusterFixture.name() + ":" + user));
            TxnOffsetCommitRequestData txnOffsetCommitRequestData = new TxnOffsetCommitRequestData();
            txnOffsetCommitRequestData.setTransactionalId(state.transactionalId);
            txnOffsetCommitRequestData.setProducerId(state.producerIdAndEpoch.producerId);
            txnOffsetCommitRequestData.setProducerEpoch(state.producerIdAndEpoch.epoch);
            txnOffsetCommitRequestData.setGroupId(state.groupId);
            if (apiVersion() >= 3) {
                txnOffsetCommitRequestData.setGenerationId(state.generationId);
                txnOffsetCommitRequestData.setMemberId(state.memberId);
            }
            ALL_TOPIC_NAMES_IN_TEST.forEach(topicName -> {
                TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic commitTopic = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic();
                commitTopic.setName(topicName);
                TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition part = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition();
                part.setPartitionIndex(0);
                part.setCommittedOffset(1);
                commitTopic.partitions().add(part);
                txnOffsetCommitRequestData.topics().add(commitTopic);
            });
            return txnOffsetCommitRequestData;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            Map<String, KafkaClient> userToClient = cluster.authenticatedClients(PASSWORDS.keySet());
            for (Map.Entry<String, KafkaClient> entry : userToClient.entrySet()) {
                String username = entry.getKey();
                KafkaClient userClient = entry.getValue();
                String transactionalId = "trans-" + Uuid.randomUuid();
                String groupId = "group-" + Uuid.randomUuid();
                String groupInstanceId = groupId + "-" + username;
                KafkaClient driverClient = userClient;
                if (Objects.equals(username, "eve")) {
                    driverClient = userToClient.get("alice"); // eve does not have permissions to init producer id, so we drive the cluster using alice
                }
                KafkaDriver kafkaDriver = new KafkaDriver(cluster, driverClient, username);
                kafkaDriver.findCoordinator(CoordinatorType.TRANSACTION, transactionalId);
                kafkaDriver.findCoordinator(CoordinatorType.GROUP, groupId);
                ProducerIdAndEpoch producerIdAndEpoch = kafkaDriver.initProducerId(transactionalId);
                JoinGroupResponseData memberIdAndGeneration = kafkaDriver.joinGroup("consumer", groupId, groupInstanceId);
                Map<String, Collection<Integer>> partitionsToAdd = ALL_TOPIC_NAMES_IN_TEST.stream().collect(Collectors.toMap(n -> n, n -> List.of(0)));
                kafkaDriver.addPartitionsToTransaction(transactionalId, producerIdAndEpoch, partitionsToAdd);
                kafkaDriver.addOffsetsToTxn(transactionalId, producerIdAndEpoch, groupId);
                TestState state = new TestState(userClient, transactionalId, groupId, producerIdAndEpoch, memberIdAndGeneration.generationId(),
                        memberIdAndGeneration.memberId());
                testStatesPerClusterUser.put(cluster.name() + ":" + username, state);
            }
        }
    }

}
