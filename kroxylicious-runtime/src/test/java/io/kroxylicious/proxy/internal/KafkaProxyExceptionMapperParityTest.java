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
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.AddOffsetsToTxnRequestData;
import io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData;
import io.kroxylicious.kafka.common.message.AddRaftVoterRequestData;
import io.kroxylicious.kafka.common.message.AllocateProducerIdsRequestData;
import io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData;
import io.kroxylicious.kafka.common.message.AlterConfigsRequestData;
import io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData;
import io.kroxylicious.kafka.common.message.AlterPartitionRequestData;
import io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData;
import io.kroxylicious.kafka.common.message.AlterShareGroupOffsetsRequestData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.AssignReplicasToDirsRequestData;
import io.kroxylicious.kafka.common.message.BeginQuorumEpochRequestData;
import io.kroxylicious.kafka.common.message.BrokerHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.BrokerRegistrationRequestData;
import io.kroxylicious.kafka.common.message.ConsumerGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.ControllerRegistrationRequestData;
import io.kroxylicious.kafka.common.message.CreateAclsRequestData;
import io.kroxylicious.kafka.common.message.CreateDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.CreatePartitionsRequestData;
import io.kroxylicious.kafka.common.message.CreateTopicsRequestData;
import io.kroxylicious.kafka.common.message.DeleteAclsRequestData;
import io.kroxylicious.kafka.common.message.DeleteGroupsRequestData;
import io.kroxylicious.kafka.common.message.DeleteRecordsRequestData;
import io.kroxylicious.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import io.kroxylicious.kafka.common.message.DeleteShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.DeleteTopicsRequestData;
import io.kroxylicious.kafka.common.message.DescribeAclsRequestData;
import io.kroxylicious.kafka.common.message.DescribeClientQuotasRequestData;
import io.kroxylicious.kafka.common.message.DescribeClusterRequestData;
import io.kroxylicious.kafka.common.message.DescribeConfigsRequestData;
import io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.DescribeGroupsRequestData;
import io.kroxylicious.kafka.common.message.DescribeLogDirsRequestData;
import io.kroxylicious.kafka.common.message.DescribeProducersRequestData;
import io.kroxylicious.kafka.common.message.DescribeQuorumRequestData;
import io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import io.kroxylicious.kafka.common.message.DescribeTopicPartitionsRequestData;
import io.kroxylicious.kafka.common.message.DescribeTransactionsRequestData;
import io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.ElectLeadersRequestData;
import io.kroxylicious.kafka.common.message.EndQuorumEpochRequestData;
import io.kroxylicious.kafka.common.message.EndTxnRequestData;
import io.kroxylicious.kafka.common.message.EnvelopeRequestData;
import io.kroxylicious.kafka.common.message.ExpireDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchSnapshotRequestData;
import io.kroxylicious.kafka.common.message.FindCoordinatorRequestData;
import io.kroxylicious.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import io.kroxylicious.kafka.common.message.HeartbeatRequestData;
import io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData;
import io.kroxylicious.kafka.common.message.InitProducerIdRequestData;
import io.kroxylicious.kafka.common.message.InitializeShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.JoinGroupRequestData;
import io.kroxylicious.kafka.common.message.LeaveGroupRequestData;
import io.kroxylicious.kafka.common.message.ListConfigResourcesRequestData;
import io.kroxylicious.kafka.common.message.ListGroupsRequestData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.ListPartitionReassignmentsRequestData;
import io.kroxylicious.kafka.common.message.ListTransactionsRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.OffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.OffsetDeleteRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchRequestData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.PushTelemetryRequestData;
import io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import io.kroxylicious.kafka.common.message.RemoveRaftVoterRequestData;
import io.kroxylicious.kafka.common.message.RenewDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.ShareAcknowledgeRequestData;
import io.kroxylicious.kafka.common.message.ShareFetchRequestData;
import io.kroxylicious.kafka.common.message.ShareGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.ShareGroupHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.StreamsGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.StreamsGroupHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.SyncGroupRequestData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.UnregisterBrokerRequestData;
import io.kroxylicious.kafka.common.message.UpdateFeaturesRequestData;
import io.kroxylicious.kafka.common.message.UpdateRaftVoterRequestData;
import io.kroxylicious.kafka.common.message.VoteRequestData;
import io.kroxylicious.kafka.common.message.WriteShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupStateRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitializeShareGroupStateRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListConfigResourcesRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateRaftVoterRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential test: for every {@link ApiKeys} that {@link KafkaProxyExceptionMapper} currently handles, across every
 * version that {@code ApiKeys} supports, asserts that the factory's output is identical to kafka-clients' own
 * {@code AbstractRequest.getErrorResponse(int, Throwable)} oracle.
 * <p>
 */
class KafkaProxyExceptionMapperParityTest {

    private static final Errors ERROR = Errors.UNKNOWN_SERVER_ERROR;
    private static final String MESSAGE = "boom";

    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Uuid KAFKA_TOPIC_ID = new Uuid(TOPIC_ID.getMostSignificantBits(),
            TOPIC_ID.getLeastSignificantBits());

    record MatchedInput(ApiMessage kroxyliciousRequest, io.kroxylicious.kafka.common.protocol.ApiMessage kafkaRequest) {

    }

    private static final Map<ApiKeys, Function<Short, MatchedInput>> REQUEST_BUILDERS = requestBuilders();
    private static final Map<ApiKeys, BiFunction<io.kroxylicious.kafka.common.protocol.ApiMessage, Short, AbstractRequest>> ORACLE_BUILDERS = oracleBuilders();

    @ParameterizedTest
    @MethodSource("coveredApiKeyVersions")
    void factoryMatchesKafkaClientsOracle(ApiKeys apiKey, short version) {
        // Given
        MatchedInput requests = REQUEST_BUILDERS.get(apiKey).apply(version);
        AbstractRequest oracleRequest = ORACLE_BUILDERS.get(apiKey).apply(requests.kafkaRequest(), version);
        io.kroxylicious.kafka.common.protocol.ApiMessage expected = oracleRequest.getErrorResponse(0, new UnknownServerException(MESSAGE)).data();

        // When
        ApiMessage actual = KafkaProxyExceptionMapper.errorResponseData(apiKey, requests.kroxyliciousRequest, version, ERROR, MESSAGE);

        // Then
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    public static Stream<Arguments> produceVersions() {
        return ApiKeys.PRODUCE.allVersions().stream().map(version -> Arguments.argumentSet("version " + version, version));
    }

    @ParameterizedTest
    @MethodSource("produceVersions")
    void produceReturnsNullWhenAcksZero(short version) {
        // Given
        ProduceRequestData request = new ProduceRequestData().setAcks((short) 0).setTimeoutMs(1000);

        // When
        ApiMessage actual = KafkaProxyExceptionMapper.errorResponseData(ApiKeys.PRODUCE, request, version, ERROR, MESSAGE);

        // Then
        assertThat(actual).isNull();
    }

    @Test
    void apiVersionsUnsupportedVersionIncludesSupportedApiKeys() {
        // Given
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        short version = ApiKeys.API_VERSIONS.latestVersion();
        io.kroxylicious.kafka.common.message.ApiVersionsRequestData kafkaRequest = new io.kroxylicious.kafka.common.message.ApiVersionsRequestData();
        ApiVersionsRequest oracleRequest = new ApiVersionsRequest(kafkaRequest, version);
        io.kroxylicious.kafka.common.protocol.ApiMessage expected = oracleRequest.getErrorResponse(0, new UnsupportedVersionException(MESSAGE)).data();

        // When
        ApiMessage actual = KafkaProxyExceptionMapper.errorResponseData(ApiKeys.API_VERSIONS, request, version, Errors.UNSUPPORTED_VERSION, MESSAGE);

        // Then
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    static Stream<Arguments> coveredApiKeyVersions() {
        return REQUEST_BUILDERS.keySet().stream().flatMap(apiKey -> {
            io.kroxylicious.kafka.common.protocol.ApiMessage sample = REQUEST_BUILDERS.get(apiKey).apply((short) 0).kafkaRequest();
            short lowest = sample.lowestSupportedVersion();
            short highest = sample.highestSupportedVersion();
            return IntStream.rangeClosed(lowest, highest)
                    .mapToObj(version -> Arguments.argumentSet(apiKey + " v" + version, apiKey, (short) version));
        });
    }

    private static Map<ApiKeys, Function<Short, MatchedInput>> requestBuilders() {
        Map<ApiKeys, Function<Short, MatchedInput>> builders = new EnumMap<>(ApiKeys.class);
        // Regular-flat: response content never reads the request body, so an empty instance suffices at every version.
        builders.put(ApiKeys.ADD_OFFSETS_TO_TXN,
                v -> new MatchedInput(new AddOffsetsToTxnRequestData(), new io.kroxylicious.kafka.common.message.AddOffsetsToTxnRequestData()));
        builders.put(ApiKeys.ADD_RAFT_VOTER, v -> new MatchedInput(new AddRaftVoterRequestData(), new io.kroxylicious.kafka.common.message.AddRaftVoterRequestData()));
        builders.put(ApiKeys.ALLOCATE_PRODUCER_IDS,
                v -> new MatchedInput(new AllocateProducerIdsRequestData(), new io.kroxylicious.kafka.common.message.AllocateProducerIdsRequestData()));
        builders.put(ApiKeys.ALTER_PARTITION, v -> new MatchedInput(new AlterPartitionRequestData(), new io.kroxylicious.kafka.common.message.AlterPartitionRequestData()));
        builders.put(ApiKeys.ASSIGN_REPLICAS_TO_DIRS,
                v -> new MatchedInput(new AssignReplicasToDirsRequestData(), new io.kroxylicious.kafka.common.message.AssignReplicasToDirsRequestData()));
        builders.put(ApiKeys.BEGIN_QUORUM_EPOCH,
                v -> new MatchedInput(new BeginQuorumEpochRequestData(), new io.kroxylicious.kafka.common.message.BeginQuorumEpochRequestData()));
        builders.put(ApiKeys.BROKER_HEARTBEAT,
                v -> new MatchedInput(new BrokerHeartbeatRequestData(), new io.kroxylicious.kafka.common.message.BrokerHeartbeatRequestData()));
        builders.put(ApiKeys.BROKER_REGISTRATION,
                v -> new MatchedInput(new BrokerRegistrationRequestData(), new io.kroxylicious.kafka.common.message.BrokerRegistrationRequestData()));
        builders.put(ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                v -> new MatchedInput(new ConsumerGroupHeartbeatRequestData(), new io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatRequestData()));
        builders.put(ApiKeys.CONTROLLER_REGISTRATION,
                v -> new MatchedInput(new ControllerRegistrationRequestData(), new io.kroxylicious.kafka.common.message.ControllerRegistrationRequestData()));
        builders.put(ApiKeys.DESCRIBE_CLIENT_QUOTAS,
                v -> new MatchedInput(new DescribeClientQuotasRequestData(), new io.kroxylicious.kafka.common.message.DescribeClientQuotasRequestData()));
        builders.put(ApiKeys.DESCRIBE_CLUSTER,
                v -> new MatchedInput(new DescribeClusterRequestData(), new io.kroxylicious.kafka.common.message.DescribeClusterRequestData()));
        builders.put(ApiKeys.DESCRIBE_LOG_DIRS,
                v -> new MatchedInput(new DescribeLogDirsRequestData(), new io.kroxylicious.kafka.common.message.DescribeLogDirsRequestData()));
        builders.put(ApiKeys.END_QUORUM_EPOCH, v -> new MatchedInput(new EndQuorumEpochRequestData(), new io.kroxylicious.kafka.common.message.EndQuorumEpochRequestData()));
        builders.put(ApiKeys.ENVELOPE, v -> new MatchedInput(new EnvelopeRequestData(), new io.kroxylicious.kafka.common.message.EnvelopeRequestData()));
        builders.put(ApiKeys.EXPIRE_DELEGATION_TOKEN,
                v -> new MatchedInput(new ExpireDelegationTokenRequestData(), new io.kroxylicious.kafka.common.message.ExpireDelegationTokenRequestData()));
        builders.put(ApiKeys.FETCH_SNAPSHOT, v -> new MatchedInput(new FetchSnapshotRequestData(), new io.kroxylicious.kafka.common.message.FetchSnapshotRequestData()));
        builders.put(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS,
                v -> new MatchedInput(new GetTelemetrySubscriptionsRequestData(), new io.kroxylicious.kafka.common.message.GetTelemetrySubscriptionsRequestData()));
        builders.put(ApiKeys.HEARTBEAT, v -> new MatchedInput(new HeartbeatRequestData(), new io.kroxylicious.kafka.common.message.HeartbeatRequestData()));
        builders.put(ApiKeys.LIST_GROUPS, v -> new MatchedInput(new ListGroupsRequestData(), new io.kroxylicious.kafka.common.message.ListGroupsRequestData()));
        builders.put(ApiKeys.LIST_TRANSACTIONS,
                v -> new MatchedInput(new ListTransactionsRequestData(), new io.kroxylicious.kafka.common.message.ListTransactionsRequestData()));
        builders.put(ApiKeys.REMOVE_RAFT_VOTER,
                v -> new MatchedInput(new RemoveRaftVoterRequestData(), new io.kroxylicious.kafka.common.message.RemoveRaftVoterRequestData()));
        builders.put(ApiKeys.RENEW_DELEGATION_TOKEN,
                v -> new MatchedInput(new RenewDelegationTokenRequestData(), new io.kroxylicious.kafka.common.message.RenewDelegationTokenRequestData()));
        builders.put(ApiKeys.SASL_AUTHENTICATE,
                v -> new MatchedInput(new SaslAuthenticateRequestData(), new io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData()));
        builders.put(ApiKeys.SASL_HANDSHAKE, v -> new MatchedInput(new SaslHandshakeRequestData(), new io.kroxylicious.kafka.common.message.SaslHandshakeRequestData()));
        builders.put(ApiKeys.SHARE_ACKNOWLEDGE,
                v -> new MatchedInput(new ShareAcknowledgeRequestData(), new io.kroxylicious.kafka.common.message.ShareAcknowledgeRequestData()));
        builders.put(ApiKeys.SHARE_GROUP_HEARTBEAT,
                v -> new MatchedInput(new ShareGroupHeartbeatRequestData(), new io.kroxylicious.kafka.common.message.ShareGroupHeartbeatRequestData()));
        builders.put(ApiKeys.STREAMS_GROUP_HEARTBEAT,
                v -> new MatchedInput(new StreamsGroupHeartbeatRequestData(), new io.kroxylicious.kafka.common.message.StreamsGroupHeartbeatRequestData()));
        builders.put(ApiKeys.UNREGISTER_BROKER,
                v -> new MatchedInput(new UnregisterBrokerRequestData(), new io.kroxylicious.kafka.common.message.UnregisterBrokerRequestData()));
        builders.put(ApiKeys.UPDATE_RAFT_VOTER,
                v -> new MatchedInput(new UpdateRaftVoterRequestData(), new io.kroxylicious.kafka.common.message.UpdateRaftVoterRequestData()));
        builders.put(ApiKeys.VOTE, v -> new MatchedInput(new VoteRequestData(), new io.kroxylicious.kafka.common.message.VoteRequestData()));

        // Array-copy: populate with real elements so per-element error stamping is actually exercised.
        builders.put(ApiKeys.DELETE_GROUPS, v -> new MatchedInput(
                new DeleteGroupsRequestData().setGroupsNames(List.of("group-1", "group-2")),
                new io.kroxylicious.kafka.common.message.DeleteGroupsRequestData().setGroupsNames(List.of("group-1", "group-2"))));
        builders.put(ApiKeys.INCREMENTAL_ALTER_CONFIGS, KafkaProxyExceptionMapperParityTest::incrementalAlterConfigsInput);
        builders.put(ApiKeys.CONSUMER_GROUP_DESCRIBE, v -> new MatchedInput(
                new ConsumerGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2")),
                new io.kroxylicious.kafka.common.message.ConsumerGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2"))));
        builders.put(ApiKeys.SHARE_GROUP_DESCRIBE, v -> new MatchedInput(
                new ShareGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2")),
                new io.kroxylicious.kafka.common.message.ShareGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2"))));
        builders.put(ApiKeys.STREAMS_GROUP_DESCRIBE, v -> new MatchedInput(
                new StreamsGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2")),
                new io.kroxylicious.kafka.common.message.StreamsGroupDescribeRequestData().setGroupIds(List.of("group-1", "group-2"))));
        builders.put(ApiKeys.CREATE_ACLS, v -> new MatchedInput(
                new CreateAclsRequestData().setCreations(List.of(validAclCreation("topic-a"), validAclCreation("topic-b"))),
                new io.kroxylicious.kafka.common.message.CreateAclsRequestData().setCreations(List.of(validAclCreationKafka("topic-a"), validAclCreationKafka("topic-b")))));
        builders.put(ApiKeys.DELETE_ACLS, v -> new MatchedInput(
                new DeleteAclsRequestData().setFilters(List.of(validDeleteAclsFilter("topic-a"), validDeleteAclsFilter("topic-b"))),
                new io.kroxylicious.kafka.common.message.DeleteAclsRequestData()
                        .setFilters(List.of(validDeleteAclsFilterKafka("topic-a"), validDeleteAclsFilterKafka("topic-b")))));
        builders.put(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, KafkaProxyExceptionMapperParityTest::describeUserScramCredentialsInput);
        builders.put(ApiKeys.CREATE_PARTITIONS, KafkaProxyExceptionMapperParityTest::createPartitionsInput);
        builders.put(ApiKeys.CREATE_TOPICS, KafkaProxyExceptionMapperParityTest::createTopicsInput);
        builders.put(ApiKeys.DESCRIBE_CONFIGS, v -> new MatchedInput(
                new DescribeConfigsRequestData().setResources(
                        List.of(new DescribeConfigsRequestData.DescribeConfigsResource().setResourceName("topic-a").setResourceType((byte) 2))),
                new io.kroxylicious.kafka.common.message.DescribeConfigsRequestData().setResources(
                        List.of(new io.kroxylicious.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource().setResourceName("topic-a")
                                .setResourceType((byte) 2)))));
        builders.put(ApiKeys.DESCRIBE_TRANSACTIONS, v -> new MatchedInput(
                new DescribeTransactionsRequestData().setTransactionalIds(List.of("txn-1", "txn-2")),
                new io.kroxylicious.kafka.common.message.DescribeTransactionsRequestData().setTransactionalIds(List.of("txn-1", "txn-2"))));
        builders.put(ApiKeys.DESCRIBE_PRODUCERS, v -> new MatchedInput(
                new DescribeProducersRequestData().setTopics(
                        List.of(new DescribeProducersRequestData.TopicRequest().setName("topic-a").setPartitionIndexes(List.of(0, 1)))),
                new io.kroxylicious.kafka.common.message.DescribeProducersRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.DescribeProducersRequestData.TopicRequest().setName("topic-a").setPartitionIndexes(List.of(0, 1))))));
        builders.put(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, v -> new MatchedInput(
                new DescribeTopicPartitionsRequestData().setTopics(List.of(new DescribeTopicPartitionsRequestData.TopicRequest().setName("topic-a"))),
                new io.kroxylicious.kafka.common.message.DescribeTopicPartitionsRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.DescribeTopicPartitionsRequestData.TopicRequest().setName("topic-a")))));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE, v -> new MatchedInput(
                new ReadShareGroupStateRequestData().setTopics(shareGroupReadTopics()),
                new io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData().setTopics(shareGroupReadTopicsKafka())));
        builders.put(ApiKeys.WRITE_SHARE_GROUP_STATE, KafkaProxyExceptionMapperParityTest::writeShareGroupStateInput);
        builders.put(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, KafkaProxyExceptionMapperParityTest::initializeShareGroupStateInput);
        builders.put(ApiKeys.DELETE_SHARE_GROUP_STATE, KafkaProxyExceptionMapperParityTest::deleteShareGroupStateInput);
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY, KafkaProxyExceptionMapperParityTest::readShareGroupStateSummaryInput);

        // Chunk 2: moderate structural/version quirks, still no field renames.
        builders.put(ApiKeys.ALTER_CLIENT_QUOTAS, KafkaProxyExceptionMapperParityTest::alterClientQuotasInput);
        builders.put(ApiKeys.ALTER_CONFIGS, KafkaProxyExceptionMapperParityTest::alterConfigsInput);
        builders.put(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, KafkaProxyExceptionMapperParityTest::alterPartitionReassignmentsInput);
        builders.put(ApiKeys.ALTER_REPLICA_LOG_DIRS, KafkaProxyExceptionMapperParityTest::alterReplicaLogDirsInput);
        builders.put(ApiKeys.ALTER_SHARE_GROUP_OFFSETS,
                v -> new MatchedInput(new AlterShareGroupOffsetsRequestData(), new io.kroxylicious.kafka.common.message.AlterShareGroupOffsetsRequestData()));
        builders.put(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, KafkaProxyExceptionMapperParityTest::alterUserScramCredentialsInput);
        builders.put(ApiKeys.CREATE_DELEGATION_TOKEN,
                v -> new MatchedInput(new CreateDelegationTokenRequestData(), new io.kroxylicious.kafka.common.message.CreateDelegationTokenRequestData()));
        builders.put(ApiKeys.DELETE_SHARE_GROUP_OFFSETS,
                v -> new MatchedInput(new DeleteShareGroupOffsetsRequestData(), new io.kroxylicious.kafka.common.message.DeleteShareGroupOffsetsRequestData()));
        builders.put(ApiKeys.DELETE_TOPICS, KafkaProxyExceptionMapperParityTest::deleteTopicsInput);
        builders.put(ApiKeys.DESCRIBE_GROUPS, v -> new MatchedInput(
                new DescribeGroupsRequestData().setGroups(List.of("group-1", "group-2")),
                new io.kroxylicious.kafka.common.message.DescribeGroupsRequestData().setGroups(List.of("group-1", "group-2"))));
        builders.put(ApiKeys.DESCRIBE_QUORUM, v -> new MatchedInput(new DescribeQuorumRequestData(), new io.kroxylicious.kafka.common.message.DescribeQuorumRequestData()));
        builders.put(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, v -> new MatchedInput(
                new DescribeShareGroupOffsetsRequestData().setGroups(List.of(
                        new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup().setGroupId("group-1"),
                        new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup().setGroupId("group-2"))),
                new io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsRequestData().setGroups(List.of(
                        new io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup().setGroupId("group-1"),
                        new io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup().setGroupId("group-2")))));
        builders.put(ApiKeys.LIST_PARTITION_REASSIGNMENTS, KafkaProxyExceptionMapperParityTest::listPartitionReassignmentsInput);
        builders.put(ApiKeys.OFFSET_COMMIT, KafkaProxyExceptionMapperParityTest::offsetCommitInput);
        builders.put(ApiKeys.OFFSET_DELETE, v -> new MatchedInput(new OffsetDeleteRequestData(), new io.kroxylicious.kafka.common.message.OffsetDeleteRequestData()));
        builders.put(ApiKeys.PUSH_TELEMETRY, v -> new MatchedInput(new PushTelemetryRequestData(), new io.kroxylicious.kafka.common.message.PushTelemetryRequestData()));
        builders.put(ApiKeys.SHARE_FETCH, v -> new MatchedInput(new ShareFetchRequestData(), new io.kroxylicious.kafka.common.message.ShareFetchRequestData()));
        builders.put(ApiKeys.UPDATE_FEATURES, v -> new MatchedInput(new UpdateFeaturesRequestData(), new io.kroxylicious.kafka.common.message.UpdateFeaturesRequestData()));

        // Chunk 3: bespoke RPCs (sentinel defaults, conditional responses, multi-era wire shapes).
        builders.put(ApiKeys.PRODUCE, KafkaProxyExceptionMapperParityTest::produceInput);
        builders.put(ApiKeys.FETCH, KafkaProxyExceptionMapperParityTest::fetchInput);
        builders.put(ApiKeys.OFFSET_FETCH, KafkaProxyExceptionMapperParityTest::offsetFetchInput);
        builders.put(ApiKeys.METADATA, v -> new MatchedInput(
                new MetadataRequestData().setTopics(List.of(new MetadataRequestData.MetadataRequestTopic().setName("topic-a").setTopicId(TOPIC_ID))),
                new io.kroxylicious.kafka.common.message.MetadataRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.MetadataRequestData.MetadataRequestTopic().setName("topic-a").setTopicId(KAFKA_TOPIC_ID)))));
        builders.put(ApiKeys.LIST_OFFSETS, KafkaProxyExceptionMapperParityTest::listOffsetsInput);
        builders.put(ApiKeys.API_VERSIONS, v -> new MatchedInput(new ApiVersionsRequestData(), new io.kroxylicious.kafka.common.message.ApiVersionsRequestData()));
        builders.put(ApiKeys.END_TXN, v -> new MatchedInput(new EndTxnRequestData(), new io.kroxylicious.kafka.common.message.EndTxnRequestData()));
        builders.put(ApiKeys.LEAVE_GROUP, v -> new MatchedInput(
                new LeaveGroupRequestData().setGroupId("group-1").setMembers(List.of(new LeaveGroupRequestData.MemberIdentity().setMemberId("member-1"))),
                new io.kroxylicious.kafka.common.message.LeaveGroupRequestData().setGroupId("group-1").setMembers(
                        List.of(new io.kroxylicious.kafka.common.message.LeaveGroupRequestData.MemberIdentity().setMemberId("member-1")))));
        builders.put(ApiKeys.LIST_CONFIG_RESOURCES, v -> new MatchedInput(
                new ListConfigResourcesRequestData().setResourceTypes(List.of(org.apache.kafka.common.config.ConfigResource.Type.CLIENT_METRICS.id())),
                new io.kroxylicious.kafka.common.message.ListConfigResourcesRequestData().setResourceTypes(
                        List.of(org.apache.kafka.common.config.ConfigResource.Type.CLIENT_METRICS.id()))));
        builders.put(ApiKeys.DESCRIBE_ACLS, KafkaProxyExceptionMapperParityTest::describeAclsInput);
        builders.put(ApiKeys.ELECT_LEADERS, KafkaProxyExceptionMapperParityTest::electLeadersInput);
        builders.put(ApiKeys.DESCRIBE_DELEGATION_TOKEN, v -> new MatchedInput(
                new DescribeDelegationTokenRequestData().setOwners(
                        List.of(new DescribeDelegationTokenRequestData.DescribeDelegationTokenOwner().setPrincipalType("User").setPrincipalName("alice"))),
                new io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData().setOwners(
                        List.of(new io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData.DescribeDelegationTokenOwner().setPrincipalType("User")
                                .setPrincipalName("alice")))));
        builders.put(ApiKeys.ADD_PARTITIONS_TO_TXN, KafkaProxyExceptionMapperParityTest::addPartitionsToTxnInput);
        builders.put(ApiKeys.DELETE_RECORDS, KafkaProxyExceptionMapperParityTest::deleteRecordsInput);
        builders.put(ApiKeys.FIND_COORDINATOR, v -> new MatchedInput(
                new FindCoordinatorRequestData().setCoordinatorKeys(List.of("key-1")).setKeyType((byte) 0),
                new io.kroxylicious.kafka.common.message.FindCoordinatorRequestData().setCoordinatorKeys(List.of("key-1")).setKeyType((byte) 0)));
        builders.put(ApiKeys.JOIN_GROUP, v -> new MatchedInput(new JoinGroupRequestData(), new io.kroxylicious.kafka.common.message.JoinGroupRequestData()));
        builders.put(ApiKeys.OFFSET_FOR_LEADER_EPOCH, KafkaProxyExceptionMapperParityTest::offsetForLeaderEpochInput);
        builders.put(ApiKeys.SYNC_GROUP, v -> new MatchedInput(new SyncGroupRequestData(), new io.kroxylicious.kafka.common.message.SyncGroupRequestData()));
        builders.put(ApiKeys.TXN_OFFSET_COMMIT, KafkaProxyExceptionMapperParityTest::txnOffsetCommitInput);
        builders.put(ApiKeys.WRITE_TXN_MARKERS, KafkaProxyExceptionMapperParityTest::writeTxnMarkersInput);
        builders.put(ApiKeys.INIT_PRODUCER_ID, v -> new MatchedInput(
                new InitProducerIdRequestData().setTransactionTimeoutMs(60000).setTransactionalId("txn-1"),
                new io.kroxylicious.kafka.common.message.InitProducerIdRequestData().setTransactionTimeoutMs(60000).setTransactionalId("txn-1")));
        return builders;
    }

    private static MatchedInput incrementalAlterConfigsInput(short version) {
        return new MatchedInput(
                new IncrementalAlterConfigsRequestData().setResources(
                        new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection(
                                List.of(new IncrementalAlterConfigsRequestData.AlterConfigsResource().setResourceName("topic-a").setResourceType((byte) 2)).iterator())),
                new io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData().setResources(
                        new io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection(
                                List.of(new io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource().setResourceName("topic-a")
                                        .setResourceType((byte) 2)).iterator())));
    }

    private static MatchedInput describeUserScramCredentialsInput(short version) {
        return new MatchedInput(
                new DescribeUserScramCredentialsRequestData().setUsers(
                        List.of(new DescribeUserScramCredentialsRequestData.UserName().setName("alice"),
                                new DescribeUserScramCredentialsRequestData.UserName().setName("bob"))),
                new io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData().setUsers(
                        List.of(new io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName().setName("alice"),
                                new io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName().setName("bob"))));
    }

    private static MatchedInput createPartitionsInput(short version) {
        return new MatchedInput(
                new CreatePartitionsRequestData().setTopics(
                        new CreatePartitionsRequestData.CreatePartitionsTopicCollection(
                                List.of(new CreatePartitionsRequestData.CreatePartitionsTopic().setName("topic-a")).iterator())),
                new io.kroxylicious.kafka.common.message.CreatePartitionsRequestData().setTopics(
                        new io.kroxylicious.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection(
                                List.of(new io.kroxylicious.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic().setName("topic-a")).iterator())));
    }

    private static MatchedInput createTopicsInput(short version) {
        return new MatchedInput(
                new CreateTopicsRequestData().setTopics(
                        new CreateTopicsRequestData.CreatableTopicCollection(
                                List.of(new CreateTopicsRequestData.CreatableTopic().setName("topic-a")).iterator())),
                new io.kroxylicious.kafka.common.message.CreateTopicsRequestData().setTopics(
                        new io.kroxylicious.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection(
                                List.of(new io.kroxylicious.kafka.common.message.CreateTopicsRequestData.CreatableTopic().setName("topic-a")).iterator())));
    }

    private static MatchedInput writeShareGroupStateInput(short version) {
        return new MatchedInput(
                new WriteShareGroupStateRequestData().setTopics(
                        List.of(new WriteShareGroupStateRequestData.WriteStateData().setTopicId(TOPIC_ID).setPartitions(
                                List.of(new WriteShareGroupStateRequestData.PartitionData().setPartition(0))))),
                new io.kroxylicious.kafka.common.message.WriteShareGroupStateRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.WriteShareGroupStateRequestData.WriteStateData().setTopicId(KAFKA_TOPIC_ID).setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.WriteShareGroupStateRequestData.PartitionData().setPartition(0))))));
    }

    private static MatchedInput initializeShareGroupStateInput(short version) {
        return new MatchedInput(
                new InitializeShareGroupStateRequestData().setTopics(
                        List.of(new InitializeShareGroupStateRequestData.InitializeStateData().setTopicId(TOPIC_ID).setPartitions(
                                List.of(new InitializeShareGroupStateRequestData.PartitionData().setPartition(0))))),
                new io.kroxylicious.kafka.common.message.InitializeShareGroupStateRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.InitializeShareGroupStateRequestData.InitializeStateData().setTopicId(KAFKA_TOPIC_ID).setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.InitializeShareGroupStateRequestData.PartitionData().setPartition(0))))));
    }

    private static MatchedInput deleteShareGroupStateInput(short version) {
        return new MatchedInput(
                new DeleteShareGroupStateRequestData().setTopics(
                        List.of(new DeleteShareGroupStateRequestData.DeleteStateData().setTopicId(TOPIC_ID).setPartitions(
                                List.of(new DeleteShareGroupStateRequestData.PartitionData().setPartition(0))))),
                new io.kroxylicious.kafka.common.message.DeleteShareGroupStateRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.DeleteShareGroupStateRequestData.DeleteStateData().setTopicId(KAFKA_TOPIC_ID).setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.DeleteShareGroupStateRequestData.PartitionData().setPartition(0))))));
    }

    private static MatchedInput readShareGroupStateSummaryInput(short version) {
        return new MatchedInput(
                new ReadShareGroupStateSummaryRequestData().setTopics(
                        List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData().setTopicId(TOPIC_ID).setPartitions(
                                List.of(new ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(0))))),
                new io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryRequestData().setTopics(
                        List.of(new io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryRequestData.ReadStateSummaryData().setTopicId(KAFKA_TOPIC_ID).setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(0))))));
    }

    private static MatchedInput alterClientQuotasInput(short version) {
        return new MatchedInput(
                new AlterClientQuotasRequestData().setEntries(List.of(
                        new AlterClientQuotasRequestData.EntryData().setEntity(List.of(
                                new AlterClientQuotasRequestData.EntityData().setEntityType("client-id").setEntityName("client-a"))))),
                new io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData().setEntries(List.of(
                        new io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData.EntryData().setEntity(List.of(
                                new io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData.EntityData().setEntityType("client-id").setEntityName("client-a"))))));
    }

    private static MatchedInput alterConfigsInput(short version) {
        return new MatchedInput(
                new AlterConfigsRequestData().setResources(
                        new AlterConfigsRequestData.AlterConfigsResourceCollection(
                                List.of(new AlterConfigsRequestData.AlterConfigsResource().setResourceName("topic-a").setResourceType((byte) 2)).iterator())),
                new io.kroxylicious.kafka.common.message.AlterConfigsRequestData().setResources(
                        new io.kroxylicious.kafka.common.message.AlterConfigsRequestData.AlterConfigsResourceCollection(
                                List.of(new io.kroxylicious.kafka.common.message.AlterConfigsRequestData.AlterConfigsResource().setResourceName("topic-a")
                                        .setResourceType((byte) 2)).iterator())));
    }

    private static MatchedInput alterPartitionReassignmentsInput(short version) {
        return new MatchedInput(
                new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("topic-a").setPartitions(List.of(
                                new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(0))))),
                new io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData().setTopics(List.of(
                        new io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("topic-a").setPartitions(List.of(
                                new io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(0))))));
    }

    private static MatchedInput alterReplicaLogDirsInput(short version) {
        return new MatchedInput(
                new AlterReplicaLogDirsRequestData().setDirs(
                        new AlterReplicaLogDirsRequestData.AlterReplicaLogDirCollection(
                                List.of(new AlterReplicaLogDirsRequestData.AlterReplicaLogDir().setPath("/data/1").setTopics(
                                        new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection(
                                                List.of(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic().setName("topic-a").setPartitions(List.of(0, 1)))
                                                        .iterator())))
                                        .iterator())),
                new io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData().setDirs(
                        new io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirCollection(
                                List.of(new io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir().setPath("/data/1").setTopics(
                                        new io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection(
                                                List.of(new io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic().setName("topic-a")
                                                        .setPartitions(List.of(0, 1)))
                                                        .iterator())))
                                        .iterator())));
    }

    private static MatchedInput alterUserScramCredentialsInput(short version) {
        return new MatchedInput(
                new AlterUserScramCredentialsRequestData()
                        .setDeletions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("alice")))
                        .setUpsertions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("bob"))),
                new io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData()
                        .setDeletions(List.of(new io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("alice")))
                        .setUpsertions(List.of(new io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("bob"))));
    }

    // Version-sensitive: v0-5 carry the deprecated flat TopicNames field; v6+ replace it with a structured
    // Topics list (name + topicId) — the two shapes are mutually exclusive on the real wire, so build
    // whichever one matches the version under test rather than populating both unconditionally.
    private static MatchedInput deleteTopicsInput(short version) {
        return version < 6
                ? new MatchedInput(
                        new DeleteTopicsRequestData().setTopicNames(List.of("topic-a", "topic-b")),
                        new io.kroxylicious.kafka.common.message.DeleteTopicsRequestData().setTopicNames(List.of("topic-a", "topic-b")))
                : new MatchedInput(
                        new DeleteTopicsRequestData().setTopics(List.of(
                                new DeleteTopicsRequestData.DeleteTopicState().setName("topic-a").setTopicId(TOPIC_ID),
                                new DeleteTopicsRequestData.DeleteTopicState().setName("topic-b").setTopicId(TOPIC_ID))),
                        new io.kroxylicious.kafka.common.message.DeleteTopicsRequestData().setTopics(List.of(
                                new io.kroxylicious.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState().setName("topic-a").setTopicId(KAFKA_TOPIC_ID),
                                new io.kroxylicious.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState().setName("topic-b").setTopicId(KAFKA_TOPIC_ID))));
    }

    private static MatchedInput listPartitionReassignmentsInput(short version) {
        return new MatchedInput(
                new ListPartitionReassignmentsRequestData().setTopics(List.of(
                        new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics().setName("topic-a").setPartitionIndexes(List.of(0, 1)))),
                new io.kroxylicious.kafka.common.message.ListPartitionReassignmentsRequestData().setTopics(List.of(
                        new io.kroxylicious.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics().setName("topic-a")
                                .setPartitionIndexes(List.of(0, 1)))));
    }

    private static MatchedInput offsetCommitInput(short version) {
        return new MatchedInput(
                new OffsetCommitRequestData().setTopics(List.of(
                        new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("topic-a").setTopicId(TOPIC_ID).setPartitions(List.of(
                                new OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(0).setCommittedOffset(10L))))),
                new io.kroxylicious.kafka.common.message.OffsetCommitRequestData().setTopics(List.of(
                        new io.kroxylicious.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic().setName("topic-a").setTopicId(KAFKA_TOPIC_ID)
                                .setPartitions(List.of(
                                        new io.kroxylicious.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(0)
                                                .setCommittedOffset(10L))))));
    }

    private static MatchedInput produceInput(short version) {
        return new MatchedInput(
                new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000).setTopicData(
                        new ProduceRequestData.TopicProduceDataCollection(List.of(new ProduceRequestData.TopicProduceData().setName("topic-a").setTopicId(TOPIC_ID)
                                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.EMPTY))))
                                .iterator())),
                new io.kroxylicious.kafka.common.message.ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000).setTopicData(
                        new io.kroxylicious.kafka.common.message.ProduceRequestData.TopicProduceDataCollection(
                                List.of(new io.kroxylicious.kafka.common.message.ProduceRequestData.TopicProduceData().setName("topic-a").setTopicId(KAFKA_TOPIC_ID)
                                        .setPartitionData(List.of(new io.kroxylicious.kafka.common.message.ProduceRequestData.PartitionProduceData().setIndex(0)
                                                .setRecords(io.kroxylicious.kafka.common.record.internal.MemoryRecords.EMPTY))))
                                        .iterator())));
    }

    private static MatchedInput fetchInput(short version) {
        return new MatchedInput(
                new FetchRequestData().setSessionId(5).setTopics(List.of(
                        new FetchRequestData.FetchTopic().setTopic("topic-a").setTopicId(TOPIC_ID).setPartitions(
                                List.of(new FetchRequestData.FetchPartition().setPartition(0))))),
                new io.kroxylicious.kafka.common.message.FetchRequestData().setSessionId(5).setTopics(List.of(
                        new io.kroxylicious.kafka.common.message.FetchRequestData.FetchTopic().setTopic("topic-a").setTopicId(KAFKA_TOPIC_ID).setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.FetchRequestData.FetchPartition().setPartition(0))))));
    }

    // Legacy (pre-v8) top-level topics and batched (v8+) groups are both marked ignorable in the schema, so
    // populating both shapes unconditionally is safe at every version — kept as a single fixture.
    private static MatchedInput offsetFetchInput(short version) {
        return new MatchedInput(
                new OffsetFetchRequestData()
                        .setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("topic-a").setPartitionIndexes(List.of(0))))
                        .setGroups(List.of(new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("group-1").setTopics(List.of(
                                new OffsetFetchRequestData.OffsetFetchRequestTopics().setName("topic-a").setTopicId(TOPIC_ID).setPartitionIndexes(List.of(0)))))),
                new io.kroxylicious.kafka.common.message.OffsetFetchRequestData()
                        .setTopics(List.of(new io.kroxylicious.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic().setName("topic-a")
                                .setPartitionIndexes(List.of(0))))
                        .setGroups(List.of(new io.kroxylicious.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("group-1").setTopics(List.of(
                                new io.kroxylicious.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics().setName("topic-a").setTopicId(KAFKA_TOPIC_ID)
                                        .setPartitionIndexes(List.of(0)))))));
    }

    private static MatchedInput listOffsetsInput(short version) {
        return new MatchedInput(
                new ListOffsetsRequestData().setReplicaId(ListOffsetsRequest.CONSUMER_REPLICA_ID)
                        .setIsolationLevel(IsolationLevel.READ_UNCOMMITTED.id())
                        .setTopics(List.of(new ListOffsetsRequestData.ListOffsetsTopic().setName("topic-a").setPartitions(
                                List.of(new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1L))))),
                new io.kroxylicious.kafka.common.message.ListOffsetsRequestData().setReplicaId(ListOffsetsRequest.CONSUMER_REPLICA_ID)
                        .setIsolationLevel(IsolationLevel.READ_UNCOMMITTED.id())
                        .setTopics(List.of(new io.kroxylicious.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic().setName("topic-a").setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1L))))));
    }

    private static MatchedInput describeAclsInput(short version) {
        return new MatchedInput(
                new DescribeAclsRequestData()
                        .setResourceTypeFilter(ResourceType.TOPIC.code())
                        .setPatternTypeFilter(PatternType.LITERAL.code())
                        .setOperation(AclOperation.ANY.code())
                        .setPermissionType(AclPermissionType.ANY.code()),
                new io.kroxylicious.kafka.common.message.DescribeAclsRequestData()
                        .setResourceTypeFilter(ResourceType.TOPIC.code())
                        .setPatternTypeFilter(PatternType.LITERAL.code())
                        .setOperation(AclOperation.ANY.code())
                        .setPermissionType(AclPermissionType.ANY.code()));
    }

    private static MatchedInput electLeadersInput(short version) {
        return new MatchedInput(
                new ElectLeadersRequestData().setElectionType((byte) 0).setTimeoutMs(1000).setTopicPartitions(
                        new ElectLeadersRequestData.TopicPartitionsCollection(
                                List.of(new ElectLeadersRequestData.TopicPartitions().setTopic("topic-a").setPartitions(List.of(0, 1))).iterator())),
                new io.kroxylicious.kafka.common.message.ElectLeadersRequestData().setElectionType((byte) 0).setTimeoutMs(1000).setTopicPartitions(
                        new io.kroxylicious.kafka.common.message.ElectLeadersRequestData.TopicPartitionsCollection(
                                List.of(new io.kroxylicious.kafka.common.message.ElectLeadersRequestData.TopicPartitions().setTopic("topic-a").setPartitions(List.of(0, 1)))
                                        .iterator())));
    }

    private static MatchedInput addPartitionsToTxnInput(short version) {
        return new MatchedInput(
                new AddPartitionsToTxnRequestData().setV3AndBelowTopics(
                        new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection(
                                List.of(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic().setName("topic-a").setPartitions(List.of(0, 1))).iterator())),
                new io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData().setV3AndBelowTopics(
                        new io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection(
                                List.of(new io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic().setName("topic-a")
                                        .setPartitions(List.of(0, 1))).iterator())));
    }

    private static MatchedInput deleteRecordsInput(short version) {
        return new MatchedInput(
                new DeleteRecordsRequestData().setTopics(List.of(
                        new DeleteRecordsRequestData.DeleteRecordsTopic().setName("topic-a").setPartitions(
                                List.of(new DeleteRecordsRequestData.DeleteRecordsPartition().setPartitionIndex(0))))),
                new io.kroxylicious.kafka.common.message.DeleteRecordsRequestData().setTopics(List.of(
                        new io.kroxylicious.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic().setName("topic-a").setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition().setPartitionIndex(0))))));
    }

    private static MatchedInput offsetForLeaderEpochInput(short version) {
        return new MatchedInput(
                new OffsetForLeaderEpochRequestData().setTopics(
                        new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection(
                                List.of(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic("topic-a").setPartitions(
                                        List.of(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(5)))).iterator())),
                new io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData().setTopics(
                        new io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection(
                                List.of(new io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic("topic-a").setPartitions(
                                        List.of(new io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(0)
                                                .setLeaderEpoch(5))))
                                        .iterator())));
    }

    private static MatchedInput txnOffsetCommitInput(short version) {
        return new MatchedInput(
                new TxnOffsetCommitRequestData().setTopics(List.of(
                        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("topic-a").setPartitions(
                                List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(0).setCommittedOffset(5L))))),
                new io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData().setTopics(List.of(
                        new io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("topic-a").setPartitions(
                                List.of(new io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(0)
                                        .setCommittedOffset(5L))))));
    }

    private static MatchedInput writeTxnMarkersInput(short version) {
        return new MatchedInput(
                new WriteTxnMarkersRequestData().setMarkers(List.of(
                        new WriteTxnMarkersRequestData.WritableTxnMarker().setProducerId(100L).setProducerEpoch((short) 0).setCoordinatorEpoch(0)
                                .setTransactionResult(true).setTopics(List.of(
                                        new WriteTxnMarkersRequestData.WritableTxnMarkerTopic().setName("topic-a").setPartitionIndexes(List.of(0)))))),
                new io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData().setMarkers(List.of(
                        new io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarker().setProducerId(100L).setProducerEpoch((short) 0)
                                .setCoordinatorEpoch(0).setTransactionResult(true).setTopics(List.of(
                                        new io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarkerTopic().setName("topic-a")
                                                .setPartitionIndexes(List.of(0)))))));
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

    private static io.kroxylicious.kafka.common.message.CreateAclsRequestData.AclCreation validAclCreationKafka(String resourceName) {
        return new io.kroxylicious.kafka.common.message.CreateAclsRequestData.AclCreation()
                .setResourceType((byte) 2) // TOPIC
                .setResourceName(resourceName)
                .setResourcePatternType((byte) 3) // LITERAL
                .setPrincipal("User:alice")
                .setHost("*")
                .setOperation((byte) 3) // READ
                .setPermissionType((byte) 3); // ALLOW
    }

    private static io.kroxylicious.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter validDeleteAclsFilterKafka(String resourceName) {
        return new io.kroxylicious.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter((byte) 2) // TOPIC
                .setResourceNameFilter(resourceName)
                .setPatternTypeFilter((byte) 3) // LITERAL
                .setPrincipalFilter("User:alice")
                .setHostFilter("*")
                .setOperation((byte) 3) // READ
                .setPermissionType((byte) 3); // ALLOW
    }

    private static List<io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData.ReadStateData> shareGroupReadTopicsKafka() {
        return List.of(new io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData.ReadStateData().setTopicId(KAFKA_TOPIC_ID).setPartitions(
                List.of(new io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData.PartitionData().setPartition(0))));
    }

    private static Map<ApiKeys, BiFunction<io.kroxylicious.kafka.common.protocol.ApiMessage, Short, AbstractRequest>> oracleBuilders() {
        Map<ApiKeys, BiFunction<io.kroxylicious.kafka.common.protocol.ApiMessage, Short, AbstractRequest>> builders = new EnumMap<>(ApiKeys.class);
        builders.put(ApiKeys.ADD_OFFSETS_TO_TXN, (data, v) -> new AddOffsetsToTxnRequest((io.kroxylicious.kafka.common.message.AddOffsetsToTxnRequestData) data, v));
        builders.put(ApiKeys.ADD_RAFT_VOTER, (data, v) -> new AddRaftVoterRequest((io.kroxylicious.kafka.common.message.AddRaftVoterRequestData) data, v));
        builders.put(ApiKeys.ALLOCATE_PRODUCER_IDS,
                (data, v) -> new AllocateProducerIdsRequest((io.kroxylicious.kafka.common.message.AllocateProducerIdsRequestData) data, v));
        builders.put(ApiKeys.ALTER_PARTITION, (data, v) -> new AlterPartitionRequest((io.kroxylicious.kafka.common.message.AlterPartitionRequestData) data, v));
        builders.put(ApiKeys.ASSIGN_REPLICAS_TO_DIRS,
                (data, v) -> new AssignReplicasToDirsRequest((io.kroxylicious.kafka.common.message.AssignReplicasToDirsRequestData) data, v));
        builders.put(ApiKeys.BEGIN_QUORUM_EPOCH,
                (data, v) -> new BeginQuorumEpochRequest.Builder((io.kroxylicious.kafka.common.message.BeginQuorumEpochRequestData) data).build(v));
        builders.put(ApiKeys.BROKER_HEARTBEAT, (data, v) -> new BrokerHeartbeatRequest((io.kroxylicious.kafka.common.message.BrokerHeartbeatRequestData) data, v));
        builders.put(ApiKeys.BROKER_REGISTRATION, (data, v) -> new BrokerRegistrationRequest((io.kroxylicious.kafka.common.message.BrokerRegistrationRequestData) data, v));
        builders.put(ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                (data, v) -> new ConsumerGroupHeartbeatRequest((io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatRequestData) data, v));
        builders.put(ApiKeys.CONTROLLER_REGISTRATION,
                (data, v) -> new ControllerRegistrationRequest((io.kroxylicious.kafka.common.message.ControllerRegistrationRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_CLIENT_QUOTAS,
                (data, v) -> new DescribeClientQuotasRequest((io.kroxylicious.kafka.common.message.DescribeClientQuotasRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_CLUSTER, (data, v) -> new DescribeClusterRequest((io.kroxylicious.kafka.common.message.DescribeClusterRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_LOG_DIRS, (data, v) -> new DescribeLogDirsRequest((io.kroxylicious.kafka.common.message.DescribeLogDirsRequestData) data, v));
        builders.put(ApiKeys.END_QUORUM_EPOCH, (data, v) -> new EndQuorumEpochRequest.Builder((io.kroxylicious.kafka.common.message.EndQuorumEpochRequestData) data).build(v));
        builders.put(ApiKeys.ENVELOPE, (data, v) -> new EnvelopeRequest((io.kroxylicious.kafka.common.message.EnvelopeRequestData) data, v));
        builders.put(ApiKeys.EXPIRE_DELEGATION_TOKEN,
                (data, v) -> new ExpireDelegationTokenRequest.Builder((io.kroxylicious.kafka.common.message.ExpireDelegationTokenRequestData) data).build(v));
        builders.put(ApiKeys.FETCH_SNAPSHOT, (data, v) -> new FetchSnapshotRequest((io.kroxylicious.kafka.common.message.FetchSnapshotRequestData) data, v));
        builders.put(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS,
                (data, v) -> new GetTelemetrySubscriptionsRequest((io.kroxylicious.kafka.common.message.GetTelemetrySubscriptionsRequestData) data, v));
        builders.put(ApiKeys.HEARTBEAT, (data, v) -> new HeartbeatRequest.Builder((io.kroxylicious.kafka.common.message.HeartbeatRequestData) data).build(v));
        builders.put(ApiKeys.LIST_GROUPS, (data, v) -> new ListGroupsRequest((io.kroxylicious.kafka.common.message.ListGroupsRequestData) data, v));
        builders.put(ApiKeys.LIST_TRANSACTIONS,
                (data, v) -> new ListTransactionsRequest.Builder((io.kroxylicious.kafka.common.message.ListTransactionsRequestData) data).build(v));
        builders.put(ApiKeys.REMOVE_RAFT_VOTER, (data, v) -> new RemoveRaftVoterRequest((io.kroxylicious.kafka.common.message.RemoveRaftVoterRequestData) data, v));
        builders.put(ApiKeys.RENEW_DELEGATION_TOKEN,
                (data, v) -> new RenewDelegationTokenRequest.Builder((io.kroxylicious.kafka.common.message.RenewDelegationTokenRequestData) data).build(v));
        builders.put(ApiKeys.SASL_AUTHENTICATE, (data, v) -> new SaslAuthenticateRequest((io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData) data, v));
        builders.put(ApiKeys.SASL_HANDSHAKE, (data, v) -> new SaslHandshakeRequest((io.kroxylicious.kafka.common.message.SaslHandshakeRequestData) data, v));
        builders.put(ApiKeys.SHARE_ACKNOWLEDGE, (data, v) -> new ShareAcknowledgeRequest((io.kroxylicious.kafka.common.message.ShareAcknowledgeRequestData) data, v));
        builders.put(ApiKeys.SHARE_GROUP_HEARTBEAT,
                (data, v) -> new ShareGroupHeartbeatRequest((io.kroxylicious.kafka.common.message.ShareGroupHeartbeatRequestData) data, v));
        builders.put(ApiKeys.STREAMS_GROUP_HEARTBEAT,
                (data, v) -> new StreamsGroupHeartbeatRequest((io.kroxylicious.kafka.common.message.StreamsGroupHeartbeatRequestData) data, v));
        builders.put(ApiKeys.UNREGISTER_BROKER, (data, v) -> new UnregisterBrokerRequest((io.kroxylicious.kafka.common.message.UnregisterBrokerRequestData) data, v));
        builders.put(ApiKeys.UPDATE_RAFT_VOTER, (data, v) -> new UpdateRaftVoterRequest((io.kroxylicious.kafka.common.message.UpdateRaftVoterRequestData) data, v));
        builders.put(ApiKeys.VOTE, (data, v) -> new VoteRequest.Builder((io.kroxylicious.kafka.common.message.VoteRequestData) data).build(v));

        builders.put(ApiKeys.DELETE_GROUPS, (data, v) -> new DeleteGroupsRequest((io.kroxylicious.kafka.common.message.DeleteGroupsRequestData) data, v));
        builders.put(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
                (data, v) -> new IncrementalAlterConfigsRequest((io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData) data, v));
        builders.put(ApiKeys.CONSUMER_GROUP_DESCRIBE,
                (data, v) -> new ConsumerGroupDescribeRequest((io.kroxylicious.kafka.common.message.ConsumerGroupDescribeRequestData) data, v));
        builders.put(ApiKeys.SHARE_GROUP_DESCRIBE, (data, v) -> new ShareGroupDescribeRequest((io.kroxylicious.kafka.common.message.ShareGroupDescribeRequestData) data, v));
        builders.put(ApiKeys.STREAMS_GROUP_DESCRIBE,
                (data, v) -> new StreamsGroupDescribeRequest((io.kroxylicious.kafka.common.message.StreamsGroupDescribeRequestData) data, v));
        builders.put(ApiKeys.CREATE_ACLS, (data, v) -> new CreateAclsRequest.Builder((io.kroxylicious.kafka.common.message.CreateAclsRequestData) data).build(v));
        builders.put(ApiKeys.DELETE_ACLS, (data, v) -> new DeleteAclsRequest.Builder((io.kroxylicious.kafka.common.message.DeleteAclsRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
                (data, v) -> new DescribeUserScramCredentialsRequest.Builder((io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData) data).build(v));
        builders.put(ApiKeys.CREATE_PARTITIONS,
                (data, v) -> new CreatePartitionsRequest.Builder((io.kroxylicious.kafka.common.message.CreatePartitionsRequestData) data).build(v));
        builders.put(ApiKeys.CREATE_TOPICS, (data, v) -> new CreateTopicsRequest((io.kroxylicious.kafka.common.message.CreateTopicsRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_CONFIGS, (data, v) -> new DescribeConfigsRequest((io.kroxylicious.kafka.common.message.DescribeConfigsRequestData) data, v));
        builders.put(ApiKeys.DESCRIBE_TRANSACTIONS,
                (data, v) -> new DescribeTransactionsRequest.Builder((io.kroxylicious.kafka.common.message.DescribeTransactionsRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_PRODUCERS,
                (data, v) -> new DescribeProducersRequest.Builder((io.kroxylicious.kafka.common.message.DescribeProducersRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_TOPIC_PARTITIONS,
                (data, v) -> new DescribeTopicPartitionsRequest((io.kroxylicious.kafka.common.message.DescribeTopicPartitionsRequestData) data, v));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE,
                (data, v) -> new ReadShareGroupStateRequest((io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.WRITE_SHARE_GROUP_STATE,
                (data, v) -> new WriteShareGroupStateRequest((io.kroxylicious.kafka.common.message.WriteShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.INITIALIZE_SHARE_GROUP_STATE,
                (data, v) -> new InitializeShareGroupStateRequest((io.kroxylicious.kafka.common.message.InitializeShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.DELETE_SHARE_GROUP_STATE,
                (data, v) -> new DeleteShareGroupStateRequest((io.kroxylicious.kafka.common.message.DeleteShareGroupStateRequestData) data, v));
        builders.put(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY,
                (data, v) -> new ReadShareGroupStateSummaryRequest((io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryRequestData) data, v));

        builders.put(ApiKeys.ALTER_CLIENT_QUOTAS, (data, v) -> new AlterClientQuotasRequest((io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData) data, v));
        builders.put(ApiKeys.ALTER_CONFIGS, (data, v) -> new AlterConfigsRequest((io.kroxylicious.kafka.common.message.AlterConfigsRequestData) data, v));
        builders.put(ApiKeys.ALTER_PARTITION_REASSIGNMENTS,
                (data, v) -> new AlterPartitionReassignmentsRequest.Builder((io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData) data)
                        .build(v));
        builders.put(ApiKeys.ALTER_REPLICA_LOG_DIRS,
                (data, v) -> new AlterReplicaLogDirsRequest((io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData) data, v));
        builders.put(ApiKeys.ALTER_SHARE_GROUP_OFFSETS,
                (data, v) -> new AlterShareGroupOffsetsRequest.Builder((io.kroxylicious.kafka.common.message.AlterShareGroupOffsetsRequestData) data).build(v));
        builders.put(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                (data, v) -> new AlterUserScramCredentialsRequest.Builder((io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData) data).build(v));
        builders.put(ApiKeys.CREATE_DELEGATION_TOKEN,
                (data, v) -> new CreateDelegationTokenRequest.Builder((io.kroxylicious.kafka.common.message.CreateDelegationTokenRequestData) data).build(v));
        builders.put(ApiKeys.DELETE_SHARE_GROUP_OFFSETS,
                (data, v) -> new DeleteShareGroupOffsetsRequest((io.kroxylicious.kafka.common.message.DeleteShareGroupOffsetsRequestData) data, v));
        builders.put(ApiKeys.DELETE_TOPICS, (data, v) -> new DeleteTopicsRequest.Builder((io.kroxylicious.kafka.common.message.DeleteTopicsRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_GROUPS, (data, v) -> new DescribeGroupsRequest.Builder((io.kroxylicious.kafka.common.message.DescribeGroupsRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_QUORUM, (data, v) -> new DescribeQuorumRequest.Builder((io.kroxylicious.kafka.common.message.DescribeQuorumRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                (data, v) -> new DescribeShareGroupOffsetsRequest((io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsRequestData) data, v));
        builders.put(ApiKeys.LIST_PARTITION_REASSIGNMENTS,
                (data, v) -> new ListPartitionReassignmentsRequest.Builder((io.kroxylicious.kafka.common.message.ListPartitionReassignmentsRequestData) data).build(v));
        builders.put(ApiKeys.OFFSET_COMMIT, (data, v) -> new OffsetCommitRequest((io.kroxylicious.kafka.common.message.OffsetCommitRequestData) data, v));
        builders.put(ApiKeys.OFFSET_DELETE, (data, v) -> new OffsetDeleteRequest((io.kroxylicious.kafka.common.message.OffsetDeleteRequestData) data, v));
        builders.put(ApiKeys.PUSH_TELEMETRY, (data, v) -> new PushTelemetryRequest((io.kroxylicious.kafka.common.message.PushTelemetryRequestData) data, v));
        builders.put(ApiKeys.SHARE_FETCH, (data, v) -> new ShareFetchRequest((io.kroxylicious.kafka.common.message.ShareFetchRequestData) data, v));
        builders.put(ApiKeys.UPDATE_FEATURES, (data, v) -> new UpdateFeaturesRequest((io.kroxylicious.kafka.common.message.UpdateFeaturesRequestData) data, v));

        builders.put(ApiKeys.PRODUCE, (data, v) -> new ProduceRequest((io.kroxylicious.kafka.common.message.ProduceRequestData) data, v));
        builders.put(ApiKeys.FETCH, (data, v) -> new FetchRequest((io.kroxylicious.kafka.common.message.FetchRequestData) data, v));
        builders.put(ApiKeys.OFFSET_FETCH,
                (data, v) -> OffsetFetchRequest.Builder.forTopicIdsOrNames((io.kroxylicious.kafka.common.message.OffsetFetchRequestData) data, false).build(v));
        builders.put(ApiKeys.METADATA, (data, v) -> new MetadataRequest((io.kroxylicious.kafka.common.message.MetadataRequestData) data, v));
        builders.put(ApiKeys.LIST_OFFSETS, (data, v) -> {
            io.kroxylicious.kafka.common.message.ListOffsetsRequestData listOffsetsRequestData = (io.kroxylicious.kafka.common.message.ListOffsetsRequestData) data;
            return ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.forId(listOffsetsRequestData.isolationLevel()))
                    .setTargetTimes(listOffsetsRequestData.topics())
                    .build(v);
        });
        builders.put(ApiKeys.API_VERSIONS, (data, v) -> new ApiVersionsRequest((io.kroxylicious.kafka.common.message.ApiVersionsRequestData) data, v));
        builders.put(ApiKeys.END_TXN,
                (data,
                 v) -> new EndTxnRequest.Builder((io.kroxylicious.kafka.common.message.EndTxnRequestData) data, v > EndTxnRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2)
                         .build(v));
        builders.put(ApiKeys.LEAVE_GROUP, (data, v) -> {
            io.kroxylicious.kafka.common.message.LeaveGroupRequestData leaveGroupRequestData = (io.kroxylicious.kafka.common.message.LeaveGroupRequestData) data;
            return new LeaveGroupRequest.Builder(leaveGroupRequestData.groupId(), leaveGroupRequestData.members()).build(v);
        });
        builders.put(ApiKeys.LIST_CONFIG_RESOURCES,
                (data, v) -> new ListConfigResourcesRequest.Builder((io.kroxylicious.kafka.common.message.ListConfigResourcesRequestData) data).build(v));
        builders.put(ApiKeys.DESCRIBE_ACLS, (data, v) -> {
            io.kroxylicious.kafka.common.message.DescribeAclsRequestData d = (io.kroxylicious.kafka.common.message.DescribeAclsRequestData) data;
            return new DescribeAclsRequest.Builder(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.fromCode(d.resourceTypeFilter()), d.resourceNameFilter(), PatternType.fromCode(d.patternTypeFilter())),
                    new AccessControlEntryFilter(d.principalFilter(), d.hostFilter(), AclOperation.fromCode(d.operation()),
                            AclPermissionType.fromCode(d.permissionType()))))
                    .build(v);
        });
        builders.put(ApiKeys.ELECT_LEADERS, (data, v) -> {
            io.kroxylicious.kafka.common.message.ElectLeadersRequestData electLeaders = (io.kroxylicious.kafka.common.message.ElectLeadersRequestData) data;
            return new ElectLeadersRequest.Builder(
                    ElectionType.valueOf(electLeaders.electionType()),
                    electLeaders.topicPartitions().stream().flatMap(
                            t -> t.partitions().stream().map(p -> new org.apache.kafka.common.TopicPartition(t.topic(), p)))
                            .toList(),
                    electLeaders.timeoutMs())
                    .build(v);
        });
        builders.put(ApiKeys.DESCRIBE_DELEGATION_TOKEN, (data, v) -> {
            io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData tokenRequestData = (io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData) data;
            return new DescribeDelegationTokenRequest.Builder(
                    tokenRequestData.owners().stream().map(o -> new KafkaPrincipal(o.principalType(), o.principalName())).toList())
                    .build(v);
        });
        builders.put(ApiKeys.ADD_PARTITIONS_TO_TXN, (data, v) -> new AddPartitionsToTxnRequest((io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData) data, v));
        builders.put(ApiKeys.DELETE_RECORDS, (data, v) -> new DeleteRecordsRequest.Builder((io.kroxylicious.kafka.common.message.DeleteRecordsRequestData) data).build(v));
        builders.put(ApiKeys.FIND_COORDINATOR,
                (data, v) -> new FindCoordinatorRequest.Builder((io.kroxylicious.kafka.common.message.FindCoordinatorRequestData) data).build(v));
        builders.put(ApiKeys.JOIN_GROUP, (data, v) -> new JoinGroupRequest((io.kroxylicious.kafka.common.message.JoinGroupRequestData) data, v));
        builders.put(ApiKeys.OFFSET_FOR_LEADER_EPOCH,
                (data, v) -> new OffsetsForLeaderEpochRequest((io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData) data, v));
        builders.put(ApiKeys.SYNC_GROUP, (data, v) -> new SyncGroupRequest((io.kroxylicious.kafka.common.message.SyncGroupRequestData) data, v));
        builders.put(ApiKeys.TXN_OFFSET_COMMIT, (data, v) -> new TxnOffsetCommitRequest((io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData) data, v));
        builders.put(ApiKeys.WRITE_TXN_MARKERS,
                (data, v) -> new WriteTxnMarkersRequest.Builder((io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData) data).build(v));
        builders.put(ApiKeys.INIT_PRODUCER_ID, (data, v) -> new InitProducerIdRequest.Builder((io.kroxylicious.kafka.common.message.InitProducerIdRequestData) data).build(v));
        return builders;
    }
}
