/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
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
import com.google.common.collect.Streams;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

class AddPartitionsToTxnTxnlIdAuthzIT extends AuthzIT {

    private static final String TOPIC_NAME = "my-topic";
    public static final String TRANSACTIONAL_ID_SUFFIX = "-transactionalId";
    public static final String BOB_TXNL_ID = BOB + TRANSACTIONAL_ID_SUFFIX;
    public static final String ALICE_TXNL_ID = ALICE + TRANSACTIONAL_ID_SUFFIX;
    private Path rulesFile;

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
                from io.kroxylicious.filter.authorization import TopicResource as Topic,
                                                                 TransactionalIdResource as TxnlId;
                allow User with name * to * Topic with name like "*";

                allow User with name = "alice" to * TxnlId with name = "%s";
                allow User with name = "bob" to {DESCRIBE, WRITE} TxnlId with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TXNL_ID, BOB_TXNL_ID));

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
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TXNL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TXNL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TXNL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.WRITE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        try {
            this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
            this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    record TestState(
                     KafkaClient superClient,
                     String transactionalId,
                     ProducerIdAndEpoch producerIdAndEpoch) {}

    List<Arguments> shouldEnforceAccessToTransactionalIds() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(
                AuthorizationFilter.minSupportedApiVersion(ApiKeys.ADD_PARTITIONS_TO_TXN),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.ADD_PARTITIONS_TO_TXN))
                .mapToObj(apiVersion -> Arguments.argumentSet("api version " + apiVersion, new AddPartitionsToTxnEquivalence((short) apiVersion)));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(
                        ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(),
                        ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.ADD_PARTITIONS_TO_TXN, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.ADD_PARTITIONS_TO_TXN, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTransactionalIds(VersionSpecificVerification<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class AddPartitionsToTxnEquivalence extends Equivalence<AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData> {

        AddPartitionsToTxnEquivalence(short apiVersion) {
            super(apiVersion);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        record TopicPartitionError(String topic, int partition, Errors errors) {}

        @Override
        public void assertUnproxiedResponses(Map<String, AddPartitionsToTxnResponseData> unproxiedResponsesByUser) {
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, BOB,
                    new TopicPartitionError(TOPIC_NAME, 0, Errors.NONE));
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, ALICE,
                    new TopicPartitionError(TOPIC_NAME, 0, Errors.NONE));
            assertThatUserHasTopicPartitions(unproxiedResponsesByUser, EVE,
                    new TopicPartitionError(TOPIC_NAME, 0, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        }

        private void assertThatUserHasTopicPartitions(Map<String, AddPartitionsToTxnResponseData> unproxiedResponsesByUser,
                                                      String user,
                                                      TopicPartitionError... topicPartitionErrors) {
            AddPartitionsToTxnResponseData addPartitionsToTxnResponseData = unproxiedResponsesByUser.get(user);
            assertThat(addPartitionsToTxnResponseData)
                    .as("For user " + user)
                    .isNotNull();
            Stream<TopicPartitionError> multiTransactionErrors = getTopicPartitionErrorsMultiTxn(addPartitionsToTxnResponseData);
            Stream<TopicPartitionError> singleTransactionErrors = getTopicPartitionErrorsSingleTxn(addPartitionsToTxnResponseData);
            assertThat(Streams.concat(multiTransactionErrors, singleTransactionErrors).collect(toSet()))
                    .as("For user " + user)
                    .containsExactlyInAnyOrder(topicPartitionErrors);
        }

        private static Stream<TopicPartitionError> getTopicPartitionErrorsMultiTxn(AddPartitionsToTxnResponseData response) {
            return response.resultsByTransaction().stream()
                    .flatMap(transaction -> transaction.topicResults().stream())
                    .flatMap(topic -> topic.resultsByPartition().stream()
                            .map(partition -> new TopicPartitionError(topic.name(), partition.partitionIndex(), Errors.forCode(partition.partitionErrorCode()))));
        }

        private static Stream<TopicPartitionError> getTopicPartitionErrorsSingleTxn(AddPartitionsToTxnResponseData response) {
            return response.resultsByTopicV3AndBelow().stream()
                    .flatMap(topic -> topic.resultsByPartition().stream()
                            .map(partition -> new TopicPartitionError(topic.name(), partition.partitionIndex(), Errors.forCode(partition.partitionErrorCode()))));
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.ADD_PARTITIONS_TO_TXN;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public AddPartitionsToTxnRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            TestState state = Objects.requireNonNull(testStatesPerClusterUser.get(clusterFixture.name() + ":" + user));
            AddPartitionsToTxnRequestData request = new AddPartitionsToTxnRequestData();
            request.setV3AndBelowProducerId(state.producerIdAndEpoch.producerId);
            request.setV3AndBelowTransactionalId(state.transactionalId);
            request.setV3AndBelowProducerEpoch(state.producerIdAndEpoch.epoch);
            ALL_TOPIC_NAMES_IN_TEST.forEach(topicName -> {
                AddPartitionsToTxnTopic topic = new AddPartitionsToTxnTopic();
                topic.setName(topicName);
                topic.setPartitions(List.of(0));
                request.v3AndBelowTopics().add(topic);
            });
            return request;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            Map<String, KafkaClient> userToClient = cluster.authenticatedClients(PASSWORDS.keySet());
            userToClient.forEach((username, kafkaClient) -> {
                // To test that the Enforcement is really denying EVE we need a producerIdAndEpoch for Eve to use in making
                // her AddPartitionsToTxn request.
                // The usual way would be via InitProducerId, but that also checks for WRITE on the transactional id
                // so here we set things up so that EVE doesn't make an InitProducerId at all
                // and will instead use ALICE's producerIdAndEpoch for her AddPartitionsToTxn request
                if (username.equals(EVE)) {
                    return;
                }
                KafkaDriver kafkaDriver = new KafkaDriver(cluster, kafkaClient, username);
                kafkaDriver.findCoordinator(CoordinatorType.TRANSACTION, username + TRANSACTIONAL_ID_SUFFIX);
                ProducerIdAndEpoch producerIdAndEpoch = kafkaDriver.initProducerId(username + TRANSACTIONAL_ID_SUFFIX);
                TestState state = new TestState(kafkaClient, username + TRANSACTIONAL_ID_SUFFIX, producerIdAndEpoch);
                testStatesPerClusterUser.put(cluster.name() + ":" + username, state);
                if (username.equals(ALICE)) {
                    testStatesPerClusterUser.put(cluster.name() + ":" + EVE, state);
                }
            });
        }
    }

}
