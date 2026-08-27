/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupStateRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.InitializeShareGroupStateRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UpdateRaftVoterRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential test: for every {@link ApiKeys} that {@link ErrorResponseFactory} currently handles, across every
 * version that {@code ApiKeys} supports, asserts that the factory's output is identical to kafka-clients' own
 * {@code AbstractRequest.getErrorResponse(int, Throwable)} oracle (constructed the same way
 * {@code KafkaProxyExceptionMapper}'s switch used to build it).
 * <p>
 * Both sides currently operate on the same {@code org.apache.kafka.common.message.*Data} classes (the
 * {@code io.kroxylicious.kafka.*} migration has not yet reached the router/filter API), so the comparison is a
 * plain recursive equality check on the generated {@code *ResponseData} objects rather than a cross-family
 * wire round-trip via {@code kroxylicious-fidelity-harness} — there is no second wire format to round-trip
 * through yet. Once the migration lands and {@link ErrorResponseFactory} is repointed at
 * {@code io.kroxylicious.kafka.*} types, this test's oracle side (still kafka-clients) will need pairing with a
 * genuine cross-family fidelity check.
 */
class ErrorResponseFactoryTest {

    private static final Errors ERROR = Errors.UNKNOWN_SERVER_ERROR;
    private static final String MESSAGE = "boom";

    private static final org.apache.kafka.common.Uuid TOPIC_ID = org.apache.kafka.common.Uuid.randomUuid();
    private static final Map<ApiKeys, Supplier<ApiMessage>> REQUEST_BUILDERS = requestBuilders();
    private static final Map<ApiKeys, BiFunction<ApiMessage, Short, AbstractRequest>> ORACLE_BUILDERS = oracleBuilders();

    @ParameterizedTest
    @MethodSource("coveredApiKeyVersions")
    void factoryMatchesKafkaClientsOracle(ApiKeys apiKey, short version) {
        // Given
        ApiMessage request = REQUEST_BUILDERS.get(apiKey).get();
        AbstractRequest oracleRequest = ORACLE_BUILDERS.get(apiKey).apply(request, version);
        ApiMessage expected = oracleRequest.getErrorResponse(0, new UnknownServerException(MESSAGE)).data();

        // When
        ApiMessage actual = ErrorResponseFactory.errorResponseData(apiKey, request, version, ERROR, MESSAGE);

        // Then
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    static Stream<Arguments> coveredApiKeyVersions() {
        return REQUEST_BUILDERS.keySet().stream().flatMap(apiKey -> {
            ApiMessage sample = REQUEST_BUILDERS.get(apiKey).get();
            short lowest = sample.lowestSupportedVersion();
            short highest = sample.highestSupportedVersion();
            return IntStream.rangeClosed(lowest, highest)
                    .mapToObj(version -> Arguments.argumentSet(apiKey + " v" + version, apiKey, (short) version));
        });
    }

    private static Map<ApiKeys, Supplier<ApiMessage>> requestBuilders() {
        Map<ApiKeys, Supplier<ApiMessage>> builders = new EnumMap<>(ApiKeys.class);
        // Regular-flat: response content never reads the request body, so an empty instance suffices.
        builders.put(ApiKeys.ADD_OFFSETS_TO_TXN, AddOffsetsToTxnRequestData::new);
        builders.put(ApiKeys.ADD_RAFT_VOTER, AddRaftVoterRequestData::new);
        builders.put(ApiKeys.ALLOCATE_PRODUCER_IDS, AllocateProducerIdsRequestData::new);
        builders.put(ApiKeys.ALTER_PARTITION, AlterPartitionRequestData::new);
        builders.put(ApiKeys.ASSIGN_REPLICAS_TO_DIRS, AssignReplicasToDirsRequestData::new);
        builders.put(ApiKeys.BEGIN_QUORUM_EPOCH, BeginQuorumEpochRequestData::new);
        builders.put(ApiKeys.BROKER_HEARTBEAT, BrokerHeartbeatRequestData::new);
        builders.put(ApiKeys.BROKER_REGISTRATION, BrokerRegistrationRequestData::new);
        builders.put(ApiKeys.CONSUMER_GROUP_HEARTBEAT, ConsumerGroupHeartbeatRequestData::new);
        builders.put(ApiKeys.CONTROLLER_REGISTRATION, ControllerRegistrationRequestData::new);
        builders.put(ApiKeys.DESCRIBE_CLIENT_QUOTAS, DescribeClientQuotasRequestData::new);
        builders.put(ApiKeys.DESCRIBE_CLUSTER, DescribeClusterRequestData::new);
        builders.put(ApiKeys.DESCRIBE_LOG_DIRS, DescribeLogDirsRequestData::new);
        builders.put(ApiKeys.END_QUORUM_EPOCH, EndQuorumEpochRequestData::new);
        builders.put(ApiKeys.ENVELOPE, EnvelopeRequestData::new);
        builders.put(ApiKeys.EXPIRE_DELEGATION_TOKEN, ExpireDelegationTokenRequestData::new);
        builders.put(ApiKeys.FETCH_SNAPSHOT, FetchSnapshotRequestData::new);
        builders.put(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, GetTelemetrySubscriptionsRequestData::new);
        builders.put(ApiKeys.HEARTBEAT, HeartbeatRequestData::new);
        builders.put(ApiKeys.LIST_GROUPS, ListGroupsRequestData::new);
        builders.put(ApiKeys.LIST_TRANSACTIONS, ListTransactionsRequestData::new);
        builders.put(ApiKeys.REMOVE_RAFT_VOTER, RemoveRaftVoterRequestData::new);
        builders.put(ApiKeys.RENEW_DELEGATION_TOKEN, RenewDelegationTokenRequestData::new);
        builders.put(ApiKeys.SASL_AUTHENTICATE, SaslAuthenticateRequestData::new);
        builders.put(ApiKeys.SASL_HANDSHAKE, SaslHandshakeRequestData::new);
        builders.put(ApiKeys.SHARE_ACKNOWLEDGE, ShareAcknowledgeRequestData::new);
        builders.put(ApiKeys.SHARE_GROUP_HEARTBEAT, ShareGroupHeartbeatRequestData::new);
        builders.put(ApiKeys.STREAMS_GROUP_HEARTBEAT, StreamsGroupHeartbeatRequestData::new);
        builders.put(ApiKeys.UNREGISTER_BROKER, UnregisterBrokerRequestData::new);
        builders.put(ApiKeys.UPDATE_RAFT_VOTER, UpdateRaftVoterRequestData::new);
        builders.put(ApiKeys.VOTE, VoteRequestData::new);

        // Array-copy: populate with real elements so per-element error stamping is actually exercised.
        builders.put(ApiKeys.CONSUMER_GROUP_DESCRIBE, () -> new ConsumerGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2")));
        builders.put(ApiKeys.SHARE_GROUP_DESCRIBE, () -> new ShareGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2")));
        builders.put(ApiKeys.STREAMS_GROUP_DESCRIBE, () -> new StreamsGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2")));
        builders.put(ApiKeys.CREATE_ACLS, () -> new CreateAclsRequestData().setCreations(
                List.of(validAclCreation("topic-a"), validAclCreation("topic-b"))));
        builders.put(ApiKeys.DELETE_ACLS, () -> new DeleteAclsRequestData().setFilters(
                List.of(validDeleteAclsFilter("topic-a"), validDeleteAclsFilter("topic-b"))));
        builders.put(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, () -> new DescribeUserScramCredentialsRequestData().setUsers(
                List.of(new DescribeUserScramCredentialsRequestData.UserName().setName("alice"),
                        new DescribeUserScramCredentialsRequestData.UserName().setName("bob"))));
        builders.put(ApiKeys.CREATE_PARTITIONS, () -> new CreatePartitionsRequestData().setTopics(
                new CreatePartitionsRequestData.CreatePartitionsTopicCollection(
                        List.of(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("topic-a")).iterator())));
        builders.put(ApiKeys.CREATE_TOPICS, () -> new CreateTopicsRequestData().setTopics(
                new CreateTopicsRequestData.CreatableTopicCollection(
                        List.of(new CreateTopicsRequestData.CreatableTopic().setName("topic-a")).iterator())));
        builders.put(ApiKeys.DESCRIBE_CONFIGS, () -> new DescribeConfigsRequestData().setResources(
                List.of(new DescribeConfigsRequestData.DescribeConfigsResource().setResourceName("topic-a").setResourceType((byte) 2))));
        builders.put(ApiKeys.DESCRIBE_TRANSACTIONS, () -> new DescribeTransactionsRequestData().setTransactionalIds(List.of("txn-1", "txn-2")));
        builders.put(ApiKeys.DESCRIBE_PRODUCERS, () -> new DescribeProducersRequestData().setTopics(
                List.of(new DescribeProducersRequestData.TopicRequest().setName("topic-a").setPartitionIndexes(List.of(0, 1)))));
        builders.put(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, () -> new DescribeTopicPartitionsRequestData().setTopics(
                List.of(new DescribeTopicPartitionsRequestData.TopicRequest().setName("topic-a"))));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE, () -> new ReadShareGroupStateRequestData().setTopics(shareGroupReadTopics()));
        builders.put(ApiKeys.WRITE_SHARE_GROUP_STATE, () -> new WriteShareGroupStateRequestData().setTopics(
                List.of(new WriteShareGroupStateRequestData.WriteStateData().setTopicId(TOPIC_ID).setPartitions(
                        List.of(new WriteShareGroupStateRequestData.PartitionData().setPartition(0))))));
        builders.put(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, () -> new InitializeShareGroupStateRequestData().setTopics(
                List.of(new InitializeShareGroupStateRequestData.InitializeStateData().setTopicId(TOPIC_ID).setPartitions(
                        List.of(new InitializeShareGroupStateRequestData.PartitionData().setPartition(0))))));
        builders.put(ApiKeys.DELETE_SHARE_GROUP_STATE, () -> new DeleteShareGroupStateRequestData().setTopics(
                List.of(new DeleteShareGroupStateRequestData.DeleteStateData().setTopicId(TOPIC_ID).setPartitions(
                        List.of(new DeleteShareGroupStateRequestData.PartitionData().setPartition(0))))));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY, () -> new ReadShareGroupStateSummaryRequestData().setTopics(
                List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData().setTopicId(TOPIC_ID).setPartitions(
                        List.of(new ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(0))))));
        return builders;
    }

    private static CreateAclsRequestData.AclCreation validAclCreation(String resourceName) {
        return new CreateAclsRequestData.AclCreation()
                .setResourceType((byte) 2) // TOPIC
                .setResourceName(resourceName)
                .setResourcePatternType((byte) 3) // LITERAL
                .setPrincipal("User:alice")
                .setHost("*")
                .setOperation((byte) 3) // READ
                .setPermissionType((byte) 3); // ALLOW
    }

    private static DeleteAclsRequestData.DeleteAclsFilter validDeleteAclsFilter(String resourceName) {
        return new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter((byte) 2) // TOPIC
                .setResourceNameFilter(resourceName)
                .setPatternTypeFilter((byte) 3) // LITERAL
                .setPrincipalFilter("User:alice")
                .setHostFilter("*")
                .setOperation((byte) 3) // READ
                .setPermissionType((byte) 3); // ALLOW
    }

    private static List<ReadShareGroupStateRequestData.ReadStateData> shareGroupReadTopics() {
        return List.of(new ReadShareGroupStateRequestData.ReadStateData().setTopicId(TOPIC_ID).setPartitions(
                List.of(new ReadShareGroupStateRequestData.PartitionData().setPartition(0))));
    }

    private static Map<ApiKeys, BiFunction<ApiMessage, Short, AbstractRequest>> oracleBuilders() {
        Map<ApiKeys, BiFunction<ApiMessage, Short, AbstractRequest>> builders = new EnumMap<>(ApiKeys.class);
        builders.put(ApiKeys.ADD_OFFSETS_TO_TXN, (data, v) -> new AddOffsetsToTxnRequest((AddOffsetsToTxnRequestData) data, v));
        builders.put(ApiKeys.ADD_RAFT_VOTER, (data, v) -> new AddRaftVoterRequest((AddRaftVoterRequestData) data, v));
        builders.put(ApiKeys.ALLOCATE_PRODUCER_IDS, (data, v) -> new AllocateProducerIdsRequest((AllocateProducerIdsRequestData) data, v));
        builders.put(ApiKeys.ALTER_PARTITION, (data, v) -> new AlterPartitionRequest((AlterPartitionRequestData) data, v));
        builders.put(ApiKeys.ASSIGN_REPLICAS_TO_DIRS, (data, v) -> new AssignReplicasToDirsRequest((AssignReplicasToDirsRequestData) data, v));
        builders.put(ApiKeys.BEGIN_QUORUM_EPOCH, (data, v) -> new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) data).build(v));
        builders.put(ApiKeys.BROKER_HEARTBEAT, (data, v) -> new BrokerHeartbeatRequest((BrokerHeartbeatRequestData) data, v));
        builders.put(ApiKeys.BROKER_REGISTRATION, (data, v) -> new BrokerRegistrationRequest((BrokerRegistrationRequestData) data, v));
        builders.put(ApiKeys.CONSUMER_GROUP_HEARTBEAT, (data, v) -> new ConsumerGroupHeartbeatRequest((ConsumerGroupHeartbeatRequestData) data, v));
        builders.put(ApiKeys.CONTROLLER_REGISTRATION, (data, v) -> new ControllerRegistrationRequest((ControllerRegistrationRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_CLIENT_QUOTAS, (data, v) -> new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_CLUSTER, (data, v) -> new DescribeClusterRequest((DescribeClusterRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_LOG_DIRS, (data, v) -> new DescribeLogDirsRequest((DescribeLogDirsRequestData) data, v));
        builders.put(ApiKeys.END_QUORUM_EPOCH, (data, v) -> new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) data).build(v));
        builders.put(ApiKeys.ENVELOPE, (data, v) -> new EnvelopeRequest((EnvelopeRequestData) data, v));
        builders.put(ApiKeys.EXPIRE_DELEGATION_TOKEN, (data, v) -> new ExpireDelegationTokenRequest.Builder((ExpireDelegationTokenRequestData) data).build(v));
        builders.put(ApiKeys.FETCH_SNAPSHOT, (data, v) -> new FetchSnapshotRequest((FetchSnapshotRequestData) data, v));
        builders.put(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, (data, v) -> new GetTelemetrySubscriptionsRequest((GetTelemetrySubscriptionsRequestData) data, v));
        builders.put(ApiKeys.HEARTBEAT, (data, v) -> new HeartbeatRequest.Builder((HeartbeatRequestData) data).build(v));
        builders.put(ApiKeys.LIST_GROUPS, (data, v) -> new ListGroupsRequest((ListGroupsRequestData) data, v));
        builders.put(ApiKeys.LIST_TRANSACTIONS, (data, v) -> new ListTransactionsRequest.Builder((ListTransactionsRequestData) data).build(v));
        builders.put(ApiKeys.REMOVE_RAFT_VOTER, (data, v) -> new RemoveRaftVoterRequest((RemoveRaftVoterRequestData) data, v));
        builders.put(ApiKeys.RENEW_DELEGATION_TOKEN, (data, v) -> new RenewDelegationTokenRequest.Builder((RenewDelegationTokenRequestData) data).build(v));
        builders.put(ApiKeys.SASL_AUTHENTICATE, (data, v) -> new SaslAuthenticateRequest((SaslAuthenticateRequestData) data, v));
        builders.put(ApiKeys.SASL_HANDSHAKE, (data, v) -> new SaslHandshakeRequest((SaslHandshakeRequestData) data, v));
        builders.put(ApiKeys.SHARE_ACKNOWLEDGE, (data, v) -> new ShareAcknowledgeRequest((ShareAcknowledgeRequestData) data, v));
        builders.put(ApiKeys.SHARE_GROUP_HEARTBEAT, (data, v) -> new ShareGroupHeartbeatRequest((ShareGroupHeartbeatRequestData) data, v));
        builders.put(ApiKeys.STREAMS_GROUP_HEARTBEAT, (data, v) -> new StreamsGroupHeartbeatRequest((StreamsGroupHeartbeatRequestData) data, v));
        builders.put(ApiKeys.UNREGISTER_BROKER, (data, v) -> new UnregisterBrokerRequest((UnregisterBrokerRequestData) data, v));
        builders.put(ApiKeys.UPDATE_RAFT_VOTER, (data, v) -> new UpdateRaftVoterRequest((UpdateRaftVoterRequestData) data, v));
        builders.put(ApiKeys.VOTE, (data, v) -> new VoteRequest.Builder((VoteRequestData) data).build(v));

        builders.put(ApiKeys.CONSUMER_GROUP_DESCRIBE, (data, v) -> new ConsumerGroupDescribeRequest((ConsumerGroupDescribeRequestData) data, v));
        builders.put(ApiKeys.SHARE_GROUP_DESCRIBE, (data, v) -> new ShareGroupDescribeRequest((ShareGroupDescribeRequestData) data, v));
        builders.put(ApiKeys.STREAMS_GROUP_DESCRIBE, (data, v) -> new StreamsGroupDescribeRequest((StreamsGroupDescribeRequestData) data, v));
        builders.put(ApiKeys.CREATE_ACLS, (data, v) -> new CreateAclsRequest.Builder((CreateAclsRequestData) data).build(v));
        builders.put(ApiKeys.DELETE_ACLS, (data, v) -> new DeleteAclsRequest.Builder((DeleteAclsRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
                (data, v) -> new DescribeUserScramCredentialsRequest.Builder((DescribeUserScramCredentialsRequestData) data).build(v));
        builders.put(ApiKeys.CREATE_PARTITIONS, (data, v) -> new CreatePartitionsRequest.Builder((CreatePartitionsRequestData) data).build(v));
        builders.put(ApiKeys.CREATE_TOPICS, (data, v) -> new CreateTopicsRequest((CreateTopicsRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_CONFIGS, (data, v) -> new DescribeConfigsRequest((DescribeConfigsRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_TRANSACTIONS, (data, v) -> new DescribeTransactionsRequest.Builder((DescribeTransactionsRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_PRODUCERS, (data, v) -> new DescribeProducersRequest.Builder((DescribeProducersRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, (data, v) -> new DescribeTopicPartitionsRequest((DescribeTopicPartitionsRequestData) data, v));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE, (data, v) -> new ReadShareGroupStateRequest((ReadShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.WRITE_SHARE_GROUP_STATE, (data, v) -> new WriteShareGroupStateRequest((WriteShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, (data, v) -> new InitializeShareGroupStateRequest((InitializeShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.DELETE_SHARE_GROUP_STATE, (data, v) -> new DeleteShareGroupStateRequest((DeleteShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY, (data, v) -> new ReadShareGroupStateSummaryRequest((ReadShareGroupStateSummaryRequestData) data, v));
        return builders;
    }
}
