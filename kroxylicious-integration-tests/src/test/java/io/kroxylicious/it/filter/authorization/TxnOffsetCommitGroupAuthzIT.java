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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.deleteTopicsAndAcls;
import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that actors must have read permissions for the group that they reference
 * in TxnOffsetCommit RPCs. Note that the actor also needs permissions to read each
 * topic in the request, and write the transactionalId, which is covered in a separate test.
 */
class TxnOffsetCommitGroupAuthzIT extends AuthzIT {

    public static final String TRANSACTIONAL_ID_PREFIX = "trans-";
    private Path rulesFile;
    private static final String TOPIC_NAME = "shared-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(TOPIC_NAME);
    private static List<AclBinding> aclBindings;
    private static final String ALICE_GROUP_PREFIX = "alice-group";
    private static final String BOB_GROUP_PREFIX = "bob-group";

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import GroupResource as Group;
                allow User with name = "alice" to * Group with name like "%s-*";
                allow User with name = "bob" to READ Group with name like "%s-*";
                allow User with name = "super" to * Group with name like "*";
                otherwise deny;
                """.formatted(ALICE_GROUP_PREFIX, BOB_GROUP_PREFIX));

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
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.READ, AclPermissionType.ALLOW)),

                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)));

    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.TXN_OFFSET_COMMIT),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.TXN_OFFSET_COMMIT)).boxed()
                .flatMap(apiVersion -> {
                    Arguments.ArgumentSet aliceGroup = Arguments.argumentSet("api version " + apiVersion + " groupPrefix " + ALICE_GROUP_PREFIX,
                            new TxnOffsetCommitEquivalence((short) (int) apiVersion, ALICE_GROUP_PREFIX));
                    Arguments.ArgumentSet bobGroup = Arguments.argumentSet("api version " + apiVersion + " groupPrefix " + BOB_GROUP_PREFIX,
                            new TxnOffsetCommitEquivalence((short) (int) apiVersion,
                                    BOB_GROUP_PREFIX));
                    return Stream.of(aliceGroup, bobGroup);
                });
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

        private final String group;
        private final String groupPrefix;
        private ProducerIdAndEpoch producerIdAndEpoch;
        private JoinGroupResponseData memberIdAndGeneration;
        private String transactionalId;

        TxnOffsetCommitEquivalence(short apiVersion, String groupPrefix) {
            super(apiVersion);
            this.groupPrefix = groupPrefix;
            this.group = this.groupPrefix + "-" + UUID.randomUUID();
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        record TopicPartitionError(String topic, int partition, Errors errors) {}

        @Override
        public void assertUnproxiedResponses(Map<String, TxnOffsetCommitResponseData> unproxiedResponsesByUser) {
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, EVE, new TopicPartitionError(TOPIC_NAME, 0, Errors.GROUP_AUTHORIZATION_FAILED));
            if (groupPrefix.equals(ALICE_GROUP_PREFIX)) {
                assertThatUserHasTopicPartitions(unproxiedResponsesByUser, ALICE, new TopicPartitionError(TOPIC_NAME, 0, Errors.NONE));
                assertThatUserHasTopicPartitions(unproxiedResponsesByUser, BOB, new TopicPartitionError(TOPIC_NAME, 0, Errors.GROUP_AUTHORIZATION_FAILED));
            }
            else if (groupPrefix.equals(BOB_GROUP_PREFIX)) {
                assertThatUserHasTopicPartitions(unproxiedResponsesByUser, ALICE, new TopicPartitionError(TOPIC_NAME, 0, Errors.GROUP_AUTHORIZATION_FAILED));
                assertThatUserHasTopicPartitions(unproxiedResponsesByUser, BOB, new TopicPartitionError(TOPIC_NAME, 0, Errors.NONE));
            }
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
            TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData();
            request.setTransactionalId(Objects.requireNonNull(transactionalId));
            request.setProducerId(Objects.requireNonNull(producerIdAndEpoch).producerId);
            request.setProducerEpoch(producerIdAndEpoch.epoch);
            request.setGroupId(group);
            if (apiVersion() >= 3) {
                request.setGenerationId(Objects.requireNonNull(memberIdAndGeneration).generationId());
                request.setMemberId(memberIdAndGeneration.memberId());
            }
            ALL_TOPIC_NAMES_IN_TEST.forEach(topicName -> {
                TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic commitTopic = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic();
                commitTopic.setName(topicName);
                TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition part = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition();
                part.setPartitionIndex(0);
                part.setCommittedOffset(1);
                commitTopic.partitions().add(part);
                request.topics().add(commitTopic);
            });
            return request;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            transactionalId = TRANSACTIONAL_ID_PREFIX + Uuid.randomUuid();
            KafkaDriver kafkaDriver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            String groupInstanceId = group + "-instanceId";
            kafkaDriver.findCoordinator(CoordinatorType.TRANSACTION, transactionalId);
            kafkaDriver.findCoordinator(CoordinatorType.GROUP, group);
            producerIdAndEpoch = kafkaDriver.initProducerId(transactionalId);
            memberIdAndGeneration = kafkaDriver.joinGroup("consumer", group, groupInstanceId);
            Map<String, Collection<Integer>> partitionsToAdd = ALL_TOPIC_NAMES_IN_TEST.stream().collect(Collectors.toMap(n -> n, n -> List.of(0)));
            kafkaDriver.addPartitionsToTransaction(transactionalId, producerIdAndEpoch, partitionsToAdd);
            kafkaDriver.addOffsetsToTxn(transactionalId, producerIdAndEpoch, group);
        }
    }

}
