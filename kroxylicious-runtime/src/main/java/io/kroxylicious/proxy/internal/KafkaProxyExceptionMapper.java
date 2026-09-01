/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import io.kroxylicious.kafka.common.message.AddOffsetsToTxnResponseData;
import io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData;
import io.kroxylicious.kafka.common.message.AddPartitionsToTxnResponseData;
import io.kroxylicious.kafka.common.message.AddRaftVoterResponseData;
import io.kroxylicious.kafka.common.message.AllocateProducerIdsResponseData;
import io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData;
import io.kroxylicious.kafka.common.message.AlterClientQuotasResponseData;
import io.kroxylicious.kafka.common.message.AlterConfigsRequestData;
import io.kroxylicious.kafka.common.message.AlterConfigsResponseData;
import io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData;
import io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsResponseData;
import io.kroxylicious.kafka.common.message.AlterPartitionResponseData;
import io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData;
import io.kroxylicious.kafka.common.message.AlterReplicaLogDirsResponseData;
import io.kroxylicious.kafka.common.message.AlterShareGroupOffsetsResponseData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsResponseData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.AssignReplicasToDirsResponseData;
import io.kroxylicious.kafka.common.message.BeginQuorumEpochResponseData;
import io.kroxylicious.kafka.common.message.BrokerHeartbeatResponseData;
import io.kroxylicious.kafka.common.message.BrokerRegistrationResponseData;
import io.kroxylicious.kafka.common.message.ConsumerGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.ConsumerGroupDescribeResponseData;
import io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import io.kroxylicious.kafka.common.message.ControllerRegistrationResponseData;
import io.kroxylicious.kafka.common.message.CreateAclsRequestData;
import io.kroxylicious.kafka.common.message.CreateAclsResponseData;
import io.kroxylicious.kafka.common.message.CreateDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.CreatePartitionsRequestData;
import io.kroxylicious.kafka.common.message.CreatePartitionsResponseData;
import io.kroxylicious.kafka.common.message.CreateTopicsRequestData;
import io.kroxylicious.kafka.common.message.CreateTopicsResponseData;
import io.kroxylicious.kafka.common.message.DeleteAclsRequestData;
import io.kroxylicious.kafka.common.message.DeleteAclsResponseData;
import io.kroxylicious.kafka.common.message.DeleteGroupsRequestData;
import io.kroxylicious.kafka.common.message.DeleteGroupsResponseData;
import io.kroxylicious.kafka.common.message.DeleteRecordsRequestData;
import io.kroxylicious.kafka.common.message.DeleteRecordsResponseData;
import io.kroxylicious.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import io.kroxylicious.kafka.common.message.DeleteShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.DeleteShareGroupStateResponseData;
import io.kroxylicious.kafka.common.message.DeleteTopicsRequestData;
import io.kroxylicious.kafka.common.message.DeleteTopicsResponseData;
import io.kroxylicious.kafka.common.message.DescribeAclsResponseData;
import io.kroxylicious.kafka.common.message.DescribeClientQuotasResponseData;
import io.kroxylicious.kafka.common.message.DescribeClusterResponseData;
import io.kroxylicious.kafka.common.message.DescribeConfigsRequestData;
import io.kroxylicious.kafka.common.message.DescribeConfigsResponseData;
import io.kroxylicious.kafka.common.message.DescribeDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.DescribeGroupsRequestData;
import io.kroxylicious.kafka.common.message.DescribeGroupsResponseData;
import io.kroxylicious.kafka.common.message.DescribeLogDirsResponseData;
import io.kroxylicious.kafka.common.message.DescribeProducersRequestData;
import io.kroxylicious.kafka.common.message.DescribeProducersResponseData;
import io.kroxylicious.kafka.common.message.DescribeQuorumResponseData;
import io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import io.kroxylicious.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import io.kroxylicious.kafka.common.message.DescribeTopicPartitionsRequestData;
import io.kroxylicious.kafka.common.message.DescribeTopicPartitionsResponseData;
import io.kroxylicious.kafka.common.message.DescribeTransactionsRequestData;
import io.kroxylicious.kafka.common.message.DescribeTransactionsResponseData;
import io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsResponseData;
import io.kroxylicious.kafka.common.message.ElectLeadersRequestData;
import io.kroxylicious.kafka.common.message.ElectLeadersResponseData;
import io.kroxylicious.kafka.common.message.EndQuorumEpochResponseData;
import io.kroxylicious.kafka.common.message.EndTxnResponseData;
import io.kroxylicious.kafka.common.message.EnvelopeResponseData;
import io.kroxylicious.kafka.common.message.ExpireDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.FetchSnapshotResponseData;
import io.kroxylicious.kafka.common.message.FindCoordinatorRequestData;
import io.kroxylicious.kafka.common.message.FindCoordinatorResponseData;
import io.kroxylicious.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import io.kroxylicious.kafka.common.message.HeartbeatResponseData;
import io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData;
import io.kroxylicious.kafka.common.message.IncrementalAlterConfigsResponseData;
import io.kroxylicious.kafka.common.message.InitProducerIdResponseData;
import io.kroxylicious.kafka.common.message.InitializeShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.InitializeShareGroupStateResponseData;
import io.kroxylicious.kafka.common.message.JoinGroupResponseData;
import io.kroxylicious.kafka.common.message.LeaveGroupResponseData;
import io.kroxylicious.kafka.common.message.ListConfigResourcesResponseData;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.ListOffsetsResponseData;
import io.kroxylicious.kafka.common.message.ListPartitionReassignmentsRequestData;
import io.kroxylicious.kafka.common.message.ListPartitionReassignmentsResponseData;
import io.kroxylicious.kafka.common.message.ListTransactionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.OffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.OffsetCommitResponseData;
import io.kroxylicious.kafka.common.message.OffsetDeleteResponseData;
import io.kroxylicious.kafka.common.message.OffsetFetchRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchResponseData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceResponseData;
import io.kroxylicious.kafka.common.message.PushTelemetryResponseData;
import io.kroxylicious.kafka.common.message.ReadShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.ReadShareGroupStateResponseData;
import io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import io.kroxylicious.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import io.kroxylicious.kafka.common.message.RemoveRaftVoterResponseData;
import io.kroxylicious.kafka.common.message.RenewDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.message.ShareAcknowledgeResponseData;
import io.kroxylicious.kafka.common.message.ShareFetchResponseData;
import io.kroxylicious.kafka.common.message.ShareGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.ShareGroupDescribeResponseData;
import io.kroxylicious.kafka.common.message.ShareGroupHeartbeatResponseData;
import io.kroxylicious.kafka.common.message.StreamsGroupDescribeRequestData;
import io.kroxylicious.kafka.common.message.StreamsGroupDescribeResponseData;
import io.kroxylicious.kafka.common.message.StreamsGroupHeartbeatResponseData;
import io.kroxylicious.kafka.common.message.SyncGroupResponseData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitResponseData;
import io.kroxylicious.kafka.common.message.UnregisterBrokerResponseData;
import io.kroxylicious.kafka.common.message.UpdateFeaturesResponseData;
import io.kroxylicious.kafka.common.message.UpdateRaftVoterResponseData;
import io.kroxylicious.kafka.common.message.VoteResponseData;
import io.kroxylicious.kafka.common.message.WriteShareGroupStateRequestData;
import io.kroxylicious.kafka.common.message.WriteShareGroupStateResponseData;
import io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData;
import io.kroxylicious.kafka.common.message.WriteTxnMarkersResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * In the operation of the proxy there are various exceptions which are "anticipated" but not necessarily handled directly.
 * SSL Handshake errors are an illustrative example, where they are thrown and propagate through the netty channel without being handled.
 * <p>
 * The exception mapper provides a mechanism to register specific exceptions with function to evaluate them and if appropriate generate a message to respond to the
 * client with.
 */
public class KafkaProxyExceptionMapper {

    private static final int THROTTLE_TIME_MS = 0;

    private KafkaProxyExceptionMapper() {
    }

    /**
     * Builds the body of an error response for the given request, with error codes and messages
     * set according to the given error.
     * @param apiKey the API key of the request being answered
     * @param requestBody the body of the request being answered
     * @param apiVersion the API version of the request being answered
     * @param error the error to convey to the client
     * @param message the message to convey to the client, subject to per-RPC suppression rules
     * @return the error response body, or {@code null} if this API key sends no response (e.g. Produce with acks=0)
     */
    @Nullable
    public static ApiMessage errorResponseData(ApiKeys apiKey, ApiMessage requestBody, short apiVersion, Errors error, @Nullable String message) {
        if (error == Errors.NONE) {
            throw new IllegalArgumentException("Error responses must target a specific error code. Using NONE represents a programming error");
        }
        short code = error.code();
        return switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> new AddOffsetsToTxnResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case ADD_RAFT_VOTER -> new AddRaftVoterResponseData().setErrorCode(code).setErrorMessage(error.message()).setThrottleTimeMs(
                    THROTTLE_TIME_MS);
            case ALLOCATE_PRODUCER_IDS -> new AllocateProducerIdsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case ALTER_PARTITION -> new AlterPartitionResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case ASSIGN_REPLICAS_TO_DIRS -> new AssignReplicasToDirsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case BEGIN_QUORUM_EPOCH -> new BeginQuorumEpochResponseData().setErrorCode(code);
            case BROKER_HEARTBEAT -> new BrokerHeartbeatResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case BROKER_REGISTRATION -> new BrokerRegistrationResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case CONSUMER_GROUP_HEARTBEAT -> new ConsumerGroupHeartbeatResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case CONTROLLER_REGISTRATION -> new ControllerRegistrationResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setErrorMessage(error.message());
            case DESCRIBE_CLIENT_QUOTAS -> new DescribeClientQuotasResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setErrorMessage(apiErrorMessage(error, message)).setEntries(null);
            case DESCRIBE_CLUSTER -> new DescribeClusterResponseData().setErrorCode(code).setErrorMessage(apiErrorMessage(error, message));
            case DESCRIBE_LOG_DIRS -> new DescribeLogDirsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case END_QUORUM_EPOCH -> new EndQuorumEpochResponseData().setErrorCode(code);
            case ENVELOPE -> new EnvelopeResponseData().setErrorCode(code);
            case EXPIRE_DELEGATION_TOKEN -> new ExpireDelegationTokenResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case FETCH_SNAPSHOT -> new FetchSnapshotResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case GET_TELEMETRY_SUBSCRIPTIONS -> new GetTelemetrySubscriptionsResponseData().setErrorCode(code).setThrottleTimeMs(
                    THROTTLE_TIME_MS);
            case HEARTBEAT -> heartbeatErrorResponse(apiVersion, code);
            case LIST_GROUPS -> listGroupsErrorResponse(apiVersion, code);
            case LIST_TRANSACTIONS -> new ListTransactionsResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case REMOVE_RAFT_VOTER -> new RemoveRaftVoterResponseData().setErrorCode(code).setErrorMessage(error.message()).setThrottleTimeMs(
                    THROTTLE_TIME_MS);
            case RENEW_DELEGATION_TOKEN -> new RenewDelegationTokenResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case SASL_AUTHENTICATE -> new SaslAuthenticateResponseData().setErrorCode(code).setErrorMessage(apiErrorMessage(error, message));
            case SASL_HANDSHAKE -> new SaslHandshakeResponseData().setErrorCode(code);
            case SHARE_ACKNOWLEDGE -> new ShareAcknowledgeResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case SHARE_GROUP_HEARTBEAT -> new ShareGroupHeartbeatResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case STREAMS_GROUP_HEARTBEAT -> new StreamsGroupHeartbeatResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case UNREGISTER_BROKER -> new UnregisterBrokerResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code).setErrorMessage(message);
            case UPDATE_RAFT_VOTER -> new UpdateRaftVoterResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case VOTE -> new VoteResponseData().setErrorCode(code);

            case DELETE_GROUPS -> deleteGroupsErrorResponse((DeleteGroupsRequestData) requestBody, code);
            case INCREMENTAL_ALTER_CONFIGS -> incrementalAlterConfigsErrorResponse((IncrementalAlterConfigsRequestData) requestBody, code,
                    apiErrorMessage(error, message));
            case CONSUMER_GROUP_DESCRIBE -> consumerGroupDescribeErrorResponse((ConsumerGroupDescribeRequestData) requestBody, code);
            case SHARE_GROUP_DESCRIBE -> shareGroupDescribeErrorResponse((ShareGroupDescribeRequestData) requestBody, code);
            case STREAMS_GROUP_DESCRIBE -> streamsGroupDescribeErrorResponse((StreamsGroupDescribeRequestData) requestBody, code);
            case CREATE_ACLS -> createAclsErrorResponse((CreateAclsRequestData) requestBody, code, apiErrorMessage(error, message));
            case DELETE_ACLS -> deleteAclsErrorResponse((DeleteAclsRequestData) requestBody, code, apiErrorMessage(error, message));
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> describeUserScramCredentialsErrorResponse((DescribeUserScramCredentialsRequestData) requestBody, code,
                    apiErrorMessage(error, message));
            case CREATE_PARTITIONS -> createPartitionsErrorResponse((CreatePartitionsRequestData) requestBody, code, apiErrorMessage(error, message));
            case CREATE_TOPICS -> createTopicsErrorResponse((CreateTopicsRequestData) requestBody, apiVersion, code, apiErrorMessage(error, message));
            case DESCRIBE_CONFIGS -> describeConfigsErrorResponse((DescribeConfigsRequestData) requestBody, code, error.message());
            case DESCRIBE_TRANSACTIONS -> describeTransactionsErrorResponse((DescribeTransactionsRequestData) requestBody, code);
            case DESCRIBE_PRODUCERS -> describeProducersErrorResponse((DescribeProducersRequestData) requestBody, code);
            case DESCRIBE_TOPIC_PARTITIONS -> describeTopicPartitionsErrorResponse((DescribeTopicPartitionsRequestData) requestBody, code);
            case READ_SHARE_GROUP_STATE -> readShareGroupStateErrorResponse((ReadShareGroupStateRequestData) requestBody, code, error.message());
            case WRITE_SHARE_GROUP_STATE -> writeShareGroupStateErrorResponse((WriteShareGroupStateRequestData) requestBody, code, error.message());
            case INITIALIZE_SHARE_GROUP_STATE -> initializeShareGroupStateErrorResponse((InitializeShareGroupStateRequestData) requestBody, code);
            case DELETE_SHARE_GROUP_STATE -> deleteShareGroupStateErrorResponse((DeleteShareGroupStateRequestData) requestBody, code);
            case READ_SHARE_GROUP_STATE_SUMMARY -> readShareGroupStateSummaryErrorResponse((ReadShareGroupStateSummaryRequestData) requestBody, code,
                    error.message());

            case ALTER_CLIENT_QUOTAS -> alterClientQuotasErrorResponse((AlterClientQuotasRequestData) requestBody, code, error.message());
            case ALTER_CONFIGS -> alterConfigsErrorResponse((AlterConfigsRequestData) requestBody, code, apiErrorMessage(error, message));
            case ALTER_PARTITION_REASSIGNMENTS -> alterPartitionReassignmentsErrorResponse((AlterPartitionReassignmentsRequestData) requestBody, code,
                    apiErrorMessage(error, message));
            case ALTER_REPLICA_LOG_DIRS -> alterReplicaLogDirsErrorResponse((AlterReplicaLogDirsRequestData) requestBody, code);
            case ALTER_SHARE_GROUP_OFFSETS -> new AlterShareGroupOffsetsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setErrorMessage(error.message());
            case ALTER_USER_SCRAM_CREDENTIALS -> alterUserScramCredentialsErrorResponse((AlterUserScramCredentialsRequestData) requestBody, code,
                    apiErrorMessage(error, message));
            case CREATE_DELEGATION_TOKEN -> createDelegationTokenErrorResponse(apiVersion, code);
            case DELETE_SHARE_GROUP_OFFSETS -> new DeleteShareGroupOffsetsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setErrorMessage(error.message());
            case DELETE_TOPICS -> deleteTopicsErrorResponse((DeleteTopicsRequestData) requestBody, apiVersion, code);
            case DESCRIBE_GROUPS -> describeGroupsErrorResponse((DescribeGroupsRequestData) requestBody, apiVersion, code);
            case DESCRIBE_QUORUM -> new DescribeQuorumResponseData().setErrorCode(code).setErrorMessage(error.message());
            case DESCRIBE_SHARE_GROUP_OFFSETS -> describeShareGroupOffsetsErrorResponse((DescribeShareGroupOffsetsRequestData) requestBody, code, error, message);
            case LIST_PARTITION_REASSIGNMENTS -> listPartitionReassignmentsErrorResponse((ListPartitionReassignmentsRequestData) requestBody, code,
                    apiErrorMessage(error, message));
            case OFFSET_COMMIT -> offsetCommitErrorResponse((OffsetCommitRequestData) requestBody, code);
            case OFFSET_DELETE -> new OffsetDeleteResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
            case PUSH_TELEMETRY -> new PushTelemetryResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case SHARE_FETCH -> new ShareFetchResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code).setAcquisitionLockTimeoutMs(0);
            case UPDATE_FEATURES -> new UpdateFeaturesResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setErrorMessage(apiErrorMessage(error, message));

            case PRODUCE -> produceErrorResponse((ProduceRequestData) requestBody, code, apiErrorMessage(error, message));
            case FETCH -> fetchErrorResponse((FetchRequestData) requestBody, apiVersion, code);
            case OFFSET_FETCH -> offsetFetchErrorResponse((OffsetFetchRequestData) requestBody, apiVersion, code);
            case METADATA -> metadataErrorResponse((MetadataRequestData) requestBody, code);
            case LIST_OFFSETS -> listOffsetsErrorResponse((ListOffsetsRequestData) requestBody, code);
            case API_VERSIONS -> apiVersionsErrorResponse(apiVersion, error, code);
            case END_TXN -> new EndTxnResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case LEAVE_GROUP -> leaveGroupErrorResponse(apiVersion, code);
            case LIST_CONFIG_RESOURCES -> new ListConfigResourcesResponseData().setErrorCode(code).setThrottleTimeMs(THROTTLE_TIME_MS);
            case DESCRIBE_ACLS -> new DescribeAclsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setErrorMessage(apiErrorMessage(error, message));
            case ELECT_LEADERS -> electLeadersErrorResponse((ElectLeadersRequestData) requestBody, apiVersion, code, apiErrorMessage(error, message));
            case DESCRIBE_DELEGATION_TOKEN -> new DescribeDelegationTokenResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code)
                    .setTokens(Collections.emptyList());
            case ADD_PARTITIONS_TO_TXN -> addPartitionsToTxnErrorResponse((AddPartitionsToTxnRequestData) requestBody, apiVersion, code);
            case DELETE_RECORDS -> deleteRecordsErrorResponse((DeleteRecordsRequestData) requestBody, code);
            case FIND_COORDINATOR -> findCoordinatorErrorResponse((FindCoordinatorRequestData) requestBody, apiVersion, error);
            case JOIN_GROUP -> joinGroupErrorResponse(apiVersion, code);
            case OFFSET_FOR_LEADER_EPOCH -> offsetForLeaderEpochErrorResponse((OffsetForLeaderEpochRequestData) requestBody, code);
            case SYNC_GROUP -> new SyncGroupResponseData().setErrorCode(code).setAssignment(new byte[0]).setThrottleTimeMs(THROTTLE_TIME_MS);
            case TXN_OFFSET_COMMIT -> txnOffsetCommitErrorResponse((TxnOffsetCommitRequestData) requestBody, code);
            case WRITE_TXN_MARKERS -> writeTxnMarkersErrorResponse((WriteTxnMarkersRequestData) requestBody, code);
            case INIT_PRODUCER_ID -> new InitProducerIdResponseData().setErrorCode(code)
                    .setProducerId(RecordBatch.NO_PRODUCER_ID).setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH).setThrottleTimeMs(
                            THROTTLE_TIME_MS);

            // Removed by Kafka 4.0 (KIP-500); never reachable from a client connection the proxy handles.
            // Handled explicitly, rather than via a catch-all default, so that adding a new ApiKeys constant
            // in a future kafka-clients upgrade is a compile error here instead of a silent runtime gap.
            case CONTROLLED_SHUTDOWN, LEADER_AND_ISR, STOP_REPLICA, UPDATE_METADATA -> throw new UnsupportedOperationException(
                    "ErrorResponseFactory does not handle APIKey: " + apiKey + " (removed by Kafka 4.0)");
        };
    }

    /**
     * Mirrors kafka-clients' {@code ApiError.fromThrowable(Throwable)}: the message is suppressed (set to
     * {@code null}) for {@link Errors#UNKNOWN_SERVER_ERROR} or when it's identical to the error code's own
     * canned message, to avoid leaking arbitrary exception text for opaque server errors.
     */
    private static @Nullable String apiErrorMessage(Errors error, @Nullable String message) {
        return error == Errors.UNKNOWN_SERVER_ERROR || error.message().equals(message) ? null : message;
    }

    private static HeartbeatResponseData heartbeatErrorResponse(short apiVersion, short code) {
        HeartbeatResponseData response = new HeartbeatResponseData().setErrorCode(code);
        if (apiVersion >= 1) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        return response;
    }

    private static ListGroupsResponseData listGroupsErrorResponse(short apiVersion, short code) {
        ListGroupsResponseData response = new ListGroupsResponseData().setGroups(Collections.emptyList()).setErrorCode(code);
        if (apiVersion >= 1) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        return response;
    }

    private static DeleteGroupsResponseData deleteGroupsErrorResponse(DeleteGroupsRequestData request, short code) {
        List<DeleteGroupsResponseData.DeletableGroupResult> results = request.groupsNames().stream()
                .map(groupId -> new DeleteGroupsResponseData.DeletableGroupResult().setGroupId(groupId).setErrorCode(code))
                .toList();
        return new DeleteGroupsResponseData().setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(results.iterator()))
                .setThrottleTimeMs(THROTTLE_TIME_MS);
    }

    private static IncrementalAlterConfigsResponseData incrementalAlterConfigsErrorResponse(IncrementalAlterConfigsRequestData request, short code,
                                                                                            @Nullable String message) {
        List<IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse> responses = request.resources().stream()
                .map(resource -> new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                        .setResourceName(resource.resourceName())
                        .setResourceType(resource.resourceType())
                        .setErrorCode(code)
                        .setErrorMessage(message))
                .toList();
        return new IncrementalAlterConfigsResponseData().setResponses(responses);
    }

    private static ConsumerGroupDescribeResponseData consumerGroupDescribeErrorResponse(ConsumerGroupDescribeRequestData request, short code) {
        ConsumerGroupDescribeResponseData response = new ConsumerGroupDescribeResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.groupIds().forEach(groupId -> response.groups().add(
                new ConsumerGroupDescribeResponseData.DescribedGroup().setGroupId(groupId).setErrorCode(code)));
        return response;
    }

    private static ShareGroupDescribeResponseData shareGroupDescribeErrorResponse(ShareGroupDescribeRequestData request, short code) {
        ShareGroupDescribeResponseData response = new ShareGroupDescribeResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.groupIds().forEach(groupId -> response.groups().add(
                new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupId).setErrorCode(code)));
        return response;
    }

    private static StreamsGroupDescribeResponseData streamsGroupDescribeErrorResponse(StreamsGroupDescribeRequestData request, short code) {
        StreamsGroupDescribeResponseData response = new StreamsGroupDescribeResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.groupIds().forEach(groupId -> response.groups().add(
                new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupId).setErrorCode(code)));
        return response;
    }

    private static CreateAclsResponseData createAclsErrorResponse(CreateAclsRequestData request, short code, @Nullable String message) {
        List<CreateAclsResponseData.AclCreationResult> results = Collections.nCopies(request.creations().size(),
                new CreateAclsResponseData.AclCreationResult().setErrorCode(code).setErrorMessage(message));
        return new CreateAclsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setResults(results);
    }

    private static DeleteAclsResponseData deleteAclsErrorResponse(DeleteAclsRequestData request, short code, @Nullable String message) {
        List<DeleteAclsResponseData.DeleteAclsFilterResult> results = Collections.nCopies(request.filters().size(),
                new DeleteAclsResponseData.DeleteAclsFilterResult().setErrorCode(code).setErrorMessage(message));
        return new DeleteAclsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setFilterResults(results);
    }

    private static DescribeUserScramCredentialsResponseData describeUserScramCredentialsErrorResponse(DescribeUserScramCredentialsRequestData request, short code,
                                                                                                      @Nullable String message) {
        DescribeUserScramCredentialsResponseData response = new DescribeUserScramCredentialsResponseData()
                .setThrottleTimeMs(THROTTLE_TIME_MS)
                .setErrorCode(code)
                .setErrorMessage(message);
        request.users().forEach(user -> response.results().add(
                new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setErrorCode(code).setErrorMessage(message)));
        return response;
    }

    private static CreatePartitionsResponseData createPartitionsErrorResponse(CreatePartitionsRequestData request, short code, @Nullable String message) {
        CreatePartitionsResponseData response = new CreatePartitionsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.topics().forEach(topic -> response.results().add(
                new CreatePartitionsResponseData.CreatePartitionsTopicResult().setName(topic.name()).setErrorCode(code).setErrorMessage(message)));
        return response;
    }

    private static CreateTopicsResponseData createTopicsErrorResponse(CreateTopicsRequestData request, short apiVersion, short code, @Nullable String message) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        if (apiVersion >= 2) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        request.topics().forEach(topic -> response.topics().add(
                new CreateTopicsResponseData.CreatableTopicResult().setName(topic.name()).setErrorCode(code).setErrorMessage(message)));
        return response;
    }

    private static DescribeConfigsResponseData describeConfigsErrorResponse(DescribeConfigsRequestData request, short code, String message) {
        List<DescribeConfigsResponseData.DescribeConfigsResult> results = request.resources().stream()
                .map(resource -> new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setErrorCode(code)
                        .setErrorMessage(message)
                        .setResourceName(resource.resourceName())
                        .setResourceType(resource.resourceType()))
                .toList();
        return new DescribeConfigsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setResults(results);
    }

    private static DescribeTransactionsResponseData describeTransactionsErrorResponse(DescribeTransactionsRequestData request, short code) {
        DescribeTransactionsResponseData response = new DescribeTransactionsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.transactionalIds().forEach(transactionalId -> response.transactionStates().add(
                new DescribeTransactionsResponseData.TransactionState().setTransactionalId(transactionalId).setErrorCode(code)));
        return response;
    }

    private static DescribeProducersResponseData describeProducersErrorResponse(DescribeProducersRequestData request, short code) {
        DescribeProducersResponseData response = new DescribeProducersResponseData();
        request.topics().forEach(topicRequest -> {
            DescribeProducersResponseData.TopicResponse topicResponse = new DescribeProducersResponseData.TopicResponse().setName(topicRequest.name());
            topicRequest.partitionIndexes().forEach(partitionId -> topicResponse.partitions().add(
                    new DescribeProducersResponseData.PartitionResponse().setPartitionIndex(partitionId).setErrorCode(code)));
            response.topics().add(topicResponse);
        });
        return response;
    }

    private static DescribeTopicPartitionsResponseData describeTopicPartitionsErrorResponse(DescribeTopicPartitionsRequestData request, short code) {
        DescribeTopicPartitionsResponseData response = new DescribeTopicPartitionsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.topics().forEach(topic -> response.topics().add(
                new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
                        .setName(topic.name())
                        .setErrorCode(code)
                        .setIsInternal(false)
                        .setPartitions(Collections.emptyList())));
        return response;
    }

    private static ReadShareGroupStateResponseData readShareGroupStateErrorResponse(ReadShareGroupStateRequestData request, short code, String message) {
        List<ReadShareGroupStateResponseData.ReadStateResult> results = new ArrayList<>();
        request.topics().forEach(topicResult -> results.add(new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                        .map(partitionData -> new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partitionData.partition())
                                .setErrorCode(code)
                                .setErrorMessage(message))
                        .toList())));
        return new ReadShareGroupStateResponseData().setResults(results);
    }

    private static WriteShareGroupStateResponseData writeShareGroupStateErrorResponse(WriteShareGroupStateRequestData request, short code, String message) {
        List<WriteShareGroupStateResponseData.WriteStateResult> results = new ArrayList<>();
        request.topics().forEach(topicResult -> results.add(new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                        .map(partitionData -> new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partitionData.partition())
                                .setErrorCode(code)
                                .setErrorMessage(message))
                        .toList())));
        return new WriteShareGroupStateResponseData().setResults(results);
    }

    private static InitializeShareGroupStateResponseData initializeShareGroupStateErrorResponse(InitializeShareGroupStateRequestData request, short code) {
        List<InitializeShareGroupStateResponseData.InitializeStateResult> results = new ArrayList<>();
        request.topics().forEach(topicResult -> results.add(new InitializeShareGroupStateResponseData.InitializeStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                        .map(partitionData -> new InitializeShareGroupStateResponseData.PartitionResult()
                                .setPartition(partitionData.partition())
                                .setErrorCode(code))
                        .toList())));
        return new InitializeShareGroupStateResponseData().setResults(results);
    }

    private static DeleteShareGroupStateResponseData deleteShareGroupStateErrorResponse(DeleteShareGroupStateRequestData request, short code) {
        List<DeleteShareGroupStateResponseData.DeleteStateResult> results = new ArrayList<>();
        request.topics().forEach(topicResult -> results.add(new DeleteShareGroupStateResponseData.DeleteStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                        .map(partitionData -> new DeleteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partitionData.partition())
                                .setErrorCode(code))
                        .toList())));
        return new DeleteShareGroupStateResponseData().setResults(results);
    }

    private static ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryErrorResponse(ReadShareGroupStateSummaryRequestData request, short code,
                                                                                                  String message) {
        List<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> results = new ArrayList<>();
        request.topics().forEach(topicResult -> results.add(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                        .map(partitionData -> new ReadShareGroupStateSummaryResponseData.PartitionResult()
                                .setPartition(partitionData.partition())
                                .setErrorCode(code)
                                .setErrorMessage(message))
                        .toList())));
        return new ReadShareGroupStateSummaryResponseData().setResults(results);
    }

    private static AlterClientQuotasResponseData alterClientQuotasErrorResponse(AlterClientQuotasRequestData request, short code, String message) {
        List<AlterClientQuotasResponseData.EntryData> entries = request.entries().stream()
                .map(entry -> new AlterClientQuotasResponseData.EntryData()
                        .setEntity(entry.entity().stream()
                                .map(entity -> new AlterClientQuotasResponseData.EntityData()
                                        .setEntityType(entity.entityType())
                                        .setEntityName(entity.entityName()))
                                .toList())
                        .setErrorCode(code)
                        .setErrorMessage(message))
                .toList();
        return new AlterClientQuotasResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setEntries(entries);
    }

    private static AlterConfigsResponseData alterConfigsErrorResponse(AlterConfigsRequestData request, short code, @Nullable String message) {
        List<AlterConfigsResponseData.AlterConfigsResourceResponse> responses = request.resources().stream()
                .map(resource -> new AlterConfigsResponseData.AlterConfigsResourceResponse()
                        .setResourceType(resource.resourceType())
                        .setResourceName(resource.resourceName())
                        .setErrorMessage(message)
                        .setErrorCode(code))
                .toList();
        return new AlterConfigsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setResponses(responses);
    }

    private static AlterPartitionReassignmentsResponseData alterPartitionReassignmentsErrorResponse(AlterPartitionReassignmentsRequestData request, short code,
                                                                                                    @Nullable String message) {
        List<AlterPartitionReassignmentsResponseData.ReassignableTopicResponse> topicResponses = request.topics().stream()
                .map(topic -> new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
                        .setName(topic.name())
                        .setPartitions(topic.partitions().stream()
                                .map(partition -> new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
                                        .setPartitionIndex(partition.partitionIndex())
                                        .setErrorCode(code)
                                        .setErrorMessage(message))
                                .toList()))
                .toList();
        return new AlterPartitionReassignmentsResponseData()
                .setResponses(topicResponses)
                .setErrorCode(code)
                .setErrorMessage(message)
                .setThrottleTimeMs(THROTTLE_TIME_MS);
    }

    private static AlterReplicaLogDirsResponseData alterReplicaLogDirsErrorResponse(AlterReplicaLogDirsRequestData request, short code) {
        List<AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult> results = request.dirs().stream()
                .flatMap(dir -> dir.topics().stream()
                        .map(topic -> new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
                                .setTopicName(topic.name())
                                .setPartitions(topic.partitions().stream()
                                        .map(partitionId -> new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                                                .setErrorCode(code)
                                                .setPartitionIndex(partitionId))
                                        .toList())))
                .toList();
        return new AlterReplicaLogDirsResponseData().setResults(results).setThrottleTimeMs(THROTTLE_TIME_MS);
    }

    private static AlterUserScramCredentialsResponseData alterUserScramCredentialsErrorResponse(AlterUserScramCredentialsRequestData request, short code,
                                                                                                @Nullable String message) {
        Set<String> users = Stream.concat(
                request.deletions().stream().map(AlterUserScramCredentialsRequestData.ScramCredentialDeletion::name),
                request.upsertions().stream().map(AlterUserScramCredentialsRequestData.ScramCredentialUpsertion::name))
                .collect(Collectors.toSet());
        List<AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult> results = users.stream().sorted()
                .map(user -> new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                        .setUser(user)
                        .setErrorCode(code)
                        .setErrorMessage(message))
                .toList();
        return new AlterUserScramCredentialsResponseData().setResults(results);
    }

    /**
     * Mirrors {@code CreateDelegationTokenResponse.prepareResponse}: the response carries no real token
     * material for an error response, so timestamps/tokenId/hmac are the same sentinels kafka-clients uses,
     * and the owner/requester principals are anonymous since the request never got as far as authenticating one.
     */
    private static CreateDelegationTokenResponseData createDelegationTokenErrorResponse(short apiVersion, short code) {
        CreateDelegationTokenResponseData response = new CreateDelegationTokenResponseData()
                .setThrottleTimeMs(THROTTLE_TIME_MS)
                .setErrorCode(code)
                .setPrincipalType(KafkaPrincipal.ANONYMOUS.getPrincipalType())
                .setPrincipalName(KafkaPrincipal.ANONYMOUS.getName())
                .setIssueTimestampMs(-1)
                .setExpiryTimestampMs(-1)
                .setMaxTimestampMs(-1)
                .setTokenId("")
                .setHmac(ByteBuffer.wrap(new byte[]{}).array());
        if (apiVersion > 2) {
            response.setTokenRequesterPrincipalType(KafkaPrincipal.ANONYMOUS.getPrincipalType())
                    .setTokenRequesterPrincipalName(KafkaPrincipal.ANONYMOUS.getName());
        }
        return response;
    }

    private static DeleteTopicsResponseData deleteTopicsErrorResponse(DeleteTopicsRequestData request, short apiVersion, short code) {
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        if (apiVersion >= 1) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        List<DeleteTopicsRequestData.DeleteTopicState> topics = apiVersion >= 6
                ? request.topics()
                : request.topicNames().stream().map(name -> new DeleteTopicsRequestData.DeleteTopicState().setName(name)).toList();
        topics.forEach(topic -> response.responses().add(
                new DeleteTopicsResponseData.DeletableTopicResult().setName(topic.name()).setTopicId(topic.topicId()).setErrorCode(code)));
        return response;
    }

    private static DescribeGroupsResponseData describeGroupsErrorResponse(DescribeGroupsRequestData request, short apiVersion, short code) {
        DescribeGroupsResponseData response = new DescribeGroupsResponseData();
        request.groups().forEach(groupId -> response.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(code)
                .setGroupState("")
                .setProtocolType("")
                .setProtocolData("")
                .setMembers(Collections.emptyList())
                .setAuthorizedOperations(Integer.MIN_VALUE)));
        if (apiVersion >= 1) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        return response;
    }

    /**
     * Mirrors {@code DescribeShareGroupOffsetsResponse}'s all-groups-error constructor: unlike
     * {@link #apiErrorMessage(Errors, String)}, the message is only suppressed for
     * {@link Errors#UNKNOWN_SERVER_ERROR} specifically, not whenever it matches the canned text.
     */
    private static DescribeShareGroupOffsetsResponseData describeShareGroupOffsetsErrorResponse(DescribeShareGroupOffsetsRequestData request, short code,
                                                                                                Errors error, @Nullable String message) {
        String errorMessage = error == Errors.UNKNOWN_SERVER_ERROR ? error.message() : message;
        List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> groups = request.groups().stream()
                .map(group -> new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
                        .setGroupId(group.groupId())
                        .setErrorCode(code)
                        .setErrorMessage(errorMessage))
                .toList();
        return new DescribeShareGroupOffsetsResponseData().setGroups(groups);
    }

    private static ListPartitionReassignmentsResponseData listPartitionReassignmentsErrorResponse(ListPartitionReassignmentsRequestData request, short code,
                                                                                                  @Nullable String message) {
        List<ListPartitionReassignmentsResponseData.OngoingTopicReassignment> topicReassignments = request.topics() == null
                ? List.of()
                : request.topics().stream()
                        .map(topic -> new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
                                .setName(topic.name())
                                .setPartitions(topic.partitionIndexes().stream()
                                        .map(partitionIndex -> new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
                                                .setPartitionIndex(partitionIndex))
                                        .toList()))
                        .toList();
        return new ListPartitionReassignmentsResponseData()
                .setTopics(topicReassignments)
                .setErrorCode(code)
                .setErrorMessage(message)
                .setThrottleTimeMs(THROTTLE_TIME_MS);
    }

    private static OffsetCommitResponseData offsetCommitErrorResponse(OffsetCommitRequestData request, short code) {
        OffsetCommitResponseData response = new OffsetCommitResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.topics().forEach(topic -> {
            OffsetCommitResponseData.OffsetCommitResponseTopic responseTopic = new OffsetCommitResponseData.OffsetCommitResponseTopic()
                    .setTopicId(topic.topicId())
                    .setName(topic.name());
            response.topics().add(responseTopic);
            topic.partitions().forEach(partition -> responseTopic.partitions().add(
                    new OffsetCommitResponseData.OffsetCommitResponsePartition().setPartitionIndex(partition.partitionIndex()).setErrorCode(code)));
        });
        return response;
    }

    /**
     * Mirrors {@code ProduceRequest.getErrorResponse}: {@code null} when {@code acks == 0} (the client doesn't
     * want a response), otherwise one response per distinct (topicId, partition, name) key deduplicated via a
     * {@link HashMap} exactly as kafka-clients does internally (via its {@code partitionSizes()}), so that the
     * insertion order into the response topic collection matches the oracle's hash-based iteration order.
     */
    @Nullable
    private static ProduceResponseData produceErrorResponse(ProduceRequestData request, short code, @Nullable String message) {
        if (request.acks() == 0) {
            return null;
        }
        Map<TopicIdPartition, Boolean> partitionKeys = new HashMap<>();
        request.topicData().forEach(topicData -> topicData.partitionData().forEach(partitionData -> partitionKeys.put(
                new TopicIdPartition(topicData.topicId(), partitionData.index(), topicData.name()), Boolean.TRUE)));
        ProduceResponseData response = new ProduceResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        partitionKeys.keySet().forEach(tpId -> {
            ProduceResponseData.TopicProduceResponse topicResponse = response.responses().find(tpId.topic(), tpId.topicId());
            if (topicResponse == null) {
                topicResponse = new ProduceResponseData.TopicProduceResponse().setName(tpId.topic()).setTopicId(tpId.topicId());
                response.responses().add(topicResponse);
            }
            topicResponse.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(tpId.partition())
                    .setRecordErrors(Collections.emptyList())
                    .setBaseOffset(-1L)
                    .setLogAppendTimeMs(RecordBatch.NO_TIMESTAMP)
                    .setLogStartOffset(-1L)
                    .setErrorMessage(message)
                    .setErrorCode(code));
        });
        return response;
    }

    private static FetchResponseData fetchErrorResponse(FetchRequestData request, short apiVersion, short code) {
        List<FetchResponseData.FetchableTopicResponse> topicResponses = new ArrayList<>();
        if (apiVersion < 13) {
            request.topics().forEach(topic -> {
                List<FetchResponseData.PartitionData> partitions = topic.partitions().stream()
                        .map(partition -> new FetchResponseData.PartitionData()
                                .setPartitionIndex(partition.partition())
                                .setErrorCode(code)
                                .setHighWatermark(-1L)
                                .setRecords(MemoryRecords.EMPTY))
                        .toList();
                topicResponses.add(new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topic.topic())
                        .setTopicId(topic.topicId())
                        .setPartitions(partitions));
            });
        }
        return new FetchResponseData()
                .setThrottleTimeMs(THROTTLE_TIME_MS)
                .setErrorCode(code)
                .setSessionId(request.sessionId())
                .setResponses(topicResponses);
    }

    /**
     * Mirrors {@code OffsetFetchRequest.getErrorResponse}'s three eras of the wire format: pre-v2 stamps every
     * partition individually (no top-level error support), v2-v7 has a single top-level error and no per-group
     * shape, v8+ is batched with one error per requested group.
     */
    private static OffsetFetchResponseData offsetFetchErrorResponse(OffsetFetchRequestData request, short apiVersion, short code) {
        if (apiVersion < 2) {
            List<OffsetFetchResponseData.OffsetFetchResponseTopic> topics = request.topics().stream()
                    .map(topic -> new OffsetFetchResponseData.OffsetFetchResponseTopic()
                            .setName(topic.name())
                            .setPartitions(topic.partitionIndexes().stream()
                                    .map(partitionIndex -> new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                            .setPartitionIndex(partitionIndex)
                                            .setErrorCode(code)
                                            .setCommittedOffset(-1L)
                                            .setMetadata("")
                                            .setCommittedLeaderEpoch(-1))
                                    .toList()))
                    .toList();
            return new OffsetFetchResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setTopics(topics);
        }
        else if (apiVersion < 8) {
            return new OffsetFetchResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
        }
        else {
            List<OffsetFetchResponseData.OffsetFetchResponseGroup> groups = request.groups().stream()
                    .map(group -> new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId(group.groupId()).setErrorCode(code))
                    .toList();
            return new OffsetFetchResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setGroups(groups);
        }
    }

    private static MetadataResponseData metadataErrorResponse(MetadataRequestData request, short code) {
        MetadataResponseData response = new MetadataResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code);
        if (request.topics() != null) {
            request.topics().forEach(topic -> response.topics().add(new MetadataResponseData.MetadataResponseTopic()
                    .setName(topic.name() == null ? "" : topic.name())
                    .setTopicId(topic.topicId())
                    .setErrorCode(code)
                    .setIsInternal(false)
                    .setPartitions(Collections.emptyList())));
        }
        return response;
    }

    private static ListOffsetsResponseData listOffsetsErrorResponse(ListOffsetsRequestData request, short code) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topics = request.topics().stream()
                .map(topic -> new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic.name())
                        .setPartitions(topic.partitions().stream()
                                .map(partition -> new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                        .setErrorCode(code)
                                        .setPartitionIndex(partition.partitionIndex())
                                        .setOffset(-1L)
                                        .setTimestamp(-1L))
                                .toList()))
                .toList();
        return new ListOffsetsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setTopics(topics);
    }

    /**
     * Mirrors {@code ApiVersionsRequest.getErrorResponse}'s KIP-511 behaviour: an {@code UNSUPPORTED_VERSION}
     * error carries the proxy's own supported {@code ApiVersions} range back to the client so it can retry at a
     * compatible version.
     */
    private static ApiVersionsResponseData apiVersionsErrorResponse(short apiVersion, Errors error, short code) {
        ApiVersionsResponseData response = new ApiVersionsResponseData().setErrorCode(code);
        if (apiVersion >= 1) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        if (error == Errors.UNSUPPORTED_VERSION) {
            ApiVersionsResponseData.ApiVersionCollection apiKeys = new ApiVersionsResponseData.ApiVersionCollection();
            apiKeys.add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(ApiKeys.API_VERSIONS.id)
                    .setMinVersion(ApiKeys.API_VERSIONS.oldestVersion())
                    .setMaxVersion(ApiKeys.API_VERSIONS.latestVersion()));
            response.setApiKeys(apiKeys);
        }
        return response;
    }

    private static LeaveGroupResponseData leaveGroupErrorResponse(short apiVersion, short code) {
        LeaveGroupResponseData response = new LeaveGroupResponseData().setErrorCode(code);
        if (apiVersion >= 1) {
            response.setThrottleTimeMs(THROTTLE_TIME_MS);
        }
        return response;
    }

    private static ElectLeadersResponseData electLeadersErrorResponse(ElectLeadersRequestData request, short apiVersion, short code, @Nullable String message) {
        ElectLeadersResponseData response = new ElectLeadersResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        if (apiVersion >= 1) {
            response.setErrorCode(code);
        }
        List<ElectLeadersResponseData.ReplicaElectionResult> electionResults = request.topicPartitions() == null
                ? List.of()
                : request.topicPartitions().stream()
                        .map(topic -> new ElectLeadersResponseData.ReplicaElectionResult()
                                .setTopic(topic.topic())
                                .setPartitionResult(topic.partitions().stream()
                                        .map(partitionId -> new ElectLeadersResponseData.PartitionResult()
                                                .setPartitionId(partitionId)
                                                .setErrorCode(code)
                                                .setErrorMessage(message))
                                        .toList()))
                        .toList();
        response.setReplicaElectionResults(electionResults);
        return response;
    }

    private static AddPartitionsToTxnResponseData addPartitionsToTxnErrorResponse(AddPartitionsToTxnRequestData request, short apiVersion, short code) {
        AddPartitionsToTxnResponseData response = new AddPartitionsToTxnResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        if (apiVersion < 4) {
            AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection results = new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection();
            request.v3AndBelowTopics().forEach(topic -> {
                AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection partitionResults = new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection();
                topic.partitions().forEach(partition -> partitionResults.add(
                        new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult().setPartitionIndex(partition).setPartitionErrorCode(code)));
                results.add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult().setName(topic.name()).setResultsByPartition(partitionResults));
            });
            response.setResultsByTopicV3AndBelow(results);
        }
        else {
            response.setErrorCode(code);
        }
        return response;
    }

    private static DeleteRecordsResponseData deleteRecordsErrorResponse(DeleteRecordsRequestData request, short code) {
        DeleteRecordsResponseData response = new DeleteRecordsResponseData().setThrottleTimeMs(THROTTLE_TIME_MS);
        request.topics().forEach(topic -> {
            DeleteRecordsResponseData.DeleteRecordsTopicResult topicResult = new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName(topic.name());
            response.topics().add(topicResult);
            topic.partitions().forEach(partition -> topicResult.partitions().add(
                    new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(partition.partitionIndex())
                            .setErrorCode(code)
                            .setLowWatermark(-1L)));
        });
        return response;
    }

    /**
     * Mirrors {@code FindCoordinatorRequest.getErrorResponse}: below the batched version it returns a single
     * unbatched coordinator error keyed by nothing (the request's own key is discarded, same as the real
     * implementation), otherwise one error entry per requested coordinator key. Note kafka-clients' own
     * implementation never actually sets throttleTimeMs here (it computes it into a value it then discards) —
     * this replicates that real, if surprising, wire behaviour rather than "fixing" it.
     */
    private static FindCoordinatorResponseData findCoordinatorErrorResponse(FindCoordinatorRequestData request, short apiVersion, Errors error) {
        if (apiVersion < 4) {
            return new FindCoordinatorResponseData()
                    .setErrorCode(error.code())
                    .setErrorMessage(error.message())
                    .setNodeId(-1)
                    .setHost("")
                    .setPort(-1);
        }
        List<FindCoordinatorResponseData.Coordinator> coordinators = request.coordinatorKeys().stream()
                .map(key -> new FindCoordinatorResponseData.Coordinator()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
                        .setKey(key)
                        .setHost("")
                        .setPort(-1)
                        .setNodeId(-1))
                .toList();
        return new FindCoordinatorResponseData().setCoordinators(coordinators);
    }

    private static JoinGroupResponseData joinGroupErrorResponse(short apiVersion, short code) {
        return new JoinGroupResponseData()
                .setThrottleTimeMs(THROTTLE_TIME_MS)
                .setErrorCode(code)
                .setGenerationId(-1)
                .setProtocolName(apiVersion >= 7 ? null : "")
                .setLeader("")
                .setMemberId("")
                .setMembers(Collections.emptyList());
    }

    private static OffsetForLeaderEpochResponseData offsetForLeaderEpochErrorResponse(OffsetForLeaderEpochRequestData request, short code) {
        OffsetForLeaderEpochResponseData response = new OffsetForLeaderEpochResponseData();
        request.topics().forEach(topic -> {
            OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult topicResult = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
                    .setTopic(topic.topic());
            response.topics().add(topicResult);
            topic.partitions().forEach(partition -> topicResult.partitions().add(
                    new OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(partition.partition())
                            .setErrorCode(code)
                            .setLeaderEpoch(-1)
                            .setEndOffset(-1L)));
        });
        return response;
    }

    private static TxnOffsetCommitResponseData txnOffsetCommitErrorResponse(TxnOffsetCommitRequestData request, short code) {
        List<TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic> topics = request.topics().stream()
                .map(topic -> new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                        .setName(topic.name())
                        .setPartitions(topic.partitions().stream()
                                .map(partition -> new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                        .setPartitionIndex(partition.partitionIndex())
                                        .setErrorCode(code))
                                .toList()))
                .toList();
        return new TxnOffsetCommitResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setTopics(topics);
    }

    /**
     * Mirrors {@code WriteTxnMarkersResponse}'s constructor from a {@code Map<producerId, Map<TopicPartition, Errors>>}:
     * marker entries and topics within a marker are deduplicated by key exactly as kafka-clients does internally
     * (last write wins for a repeated producerId; partitions merge for a repeated topic name within one marker),
     * so the response shape matches the oracle even for a request that repeats a producerId or topic name.
     */
    private static WriteTxnMarkersResponseData writeTxnMarkersErrorResponse(WriteTxnMarkersRequestData request, short code) {
        Map<Long, Map<TopicPartition, Short>> errorsByProducerId = new HashMap<>();
        request.markers().forEach(marker -> {
            Map<TopicPartition, Short> errorsPerPartition = new HashMap<>();
            marker.topics().forEach(topic -> topic.partitionIndexes().forEach(
                    partitionIndex -> errorsPerPartition.put(new TopicPartition(topic.name(), partitionIndex), code)));
            errorsByProducerId.put(marker.producerId(), errorsPerPartition);
        });

        List<WriteTxnMarkersResponseData.WritableTxnMarkerResult> markers = new ArrayList<>();
        errorsByProducerId.forEach((producerId, errorsPerPartition) -> {
            Map<String, WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult> topicsByName = new HashMap<>();
            errorsPerPartition.forEach((topicPartition, errorCode) -> {
                WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topic = topicsByName.getOrDefault(topicPartition.topic(),
                        new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult().setName(topicPartition.topic()));
                topic.partitions().add(new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                        .setErrorCode(errorCode)
                        .setPartitionIndex(topicPartition.partition()));
                topicsByName.put(topicPartition.topic(), topic);
            });
            markers.add(new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                    .setProducerId(producerId)
                    .setTopics(new ArrayList<>(topicsByName.values())));
        });
        return new WriteTxnMarkersResponseData().setMarkers(markers);
    }
}
