/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationResponseData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Invoker for Filters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
public class SpecificFilterInvoker implements FilterInvoker {

    private final Filter filter;

    public SpecificFilterInvoker(Filter filter) {
        this.filter = filter;
    }

    /**X
     * Apply the filter to the given {@code header} and {@code body} using the given {@code filterContext}.
     *
     * @param apiKey        The request api key.
     * @param apiVersion    The request api version.
     * @param header        The request header.
     * @param body          The request body.
     * @param filterContext The filter context.
     * @return request filter result
     */
    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage body,
                                                          FilterContext filterContext) {
        return switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> ((AddOffsetsToTxnRequestFilter) filter).onAddOffsetsToTxnRequest(apiVersion, header, (AddOffsetsToTxnRequestData) body,
                    filterContext);
            case ADD_PARTITIONS_TO_TXN -> ((AddPartitionsToTxnRequestFilter) filter).onAddPartitionsToTxnRequest(apiVersion, header, (AddPartitionsToTxnRequestData) body,
                    filterContext);
            case ALLOCATE_PRODUCER_IDS -> ((AllocateProducerIdsRequestFilter) filter).onAllocateProducerIdsRequest(apiVersion, header,
                    (AllocateProducerIdsRequestData) body, filterContext);
            case ALTER_CLIENT_QUOTAS -> ((AlterClientQuotasRequestFilter) filter).onAlterClientQuotasRequest(apiVersion, header, (AlterClientQuotasRequestData) body,
                    filterContext);
            case ALTER_CONFIGS -> ((AlterConfigsRequestFilter) filter).onAlterConfigsRequest(apiVersion, header, (AlterConfigsRequestData) body, filterContext);
            case ALTER_PARTITION_REASSIGNMENTS -> ((AlterPartitionReassignmentsRequestFilter) filter).onAlterPartitionReassignmentsRequest(apiVersion, header,
                    (AlterPartitionReassignmentsRequestData) body, filterContext);
            case ALTER_PARTITION -> ((AlterPartitionRequestFilter) filter).onAlterPartitionRequest(apiVersion, header, (AlterPartitionRequestData) body, filterContext);
            case ALTER_REPLICA_LOG_DIRS -> ((AlterReplicaLogDirsRequestFilter) filter).onAlterReplicaLogDirsRequest(apiVersion, header,
                    (AlterReplicaLogDirsRequestData) body, filterContext);
            case ALTER_USER_SCRAM_CREDENTIALS -> ((AlterUserScramCredentialsRequestFilter) filter).onAlterUserScramCredentialsRequest(apiVersion, header,
                    (AlterUserScramCredentialsRequestData) body,
                    filterContext);
            case API_VERSIONS -> ((ApiVersionsRequestFilter) filter).onApiVersionsRequest(apiVersion, header, (ApiVersionsRequestData) body, filterContext);
            case BEGIN_QUORUM_EPOCH -> ((BeginQuorumEpochRequestFilter) filter).onBeginQuorumEpochRequest(apiVersion, header, (BeginQuorumEpochRequestData) body,
                    filterContext);
            case BROKER_HEARTBEAT -> ((BrokerHeartbeatRequestFilter) filter).onBrokerHeartbeatRequest(apiVersion, header, (BrokerHeartbeatRequestData) body,
                    filterContext);
            case BROKER_REGISTRATION -> ((BrokerRegistrationRequestFilter) filter).onBrokerRegistrationRequest(apiVersion, header, (BrokerRegistrationRequestData) body,
                    filterContext);
            case CREATE_ACLS -> ((CreateAclsRequestFilter) filter).onCreateAclsRequest(apiVersion, header, (CreateAclsRequestData) body, filterContext);
            case CREATE_DELEGATION_TOKEN -> ((CreateDelegationTokenRequestFilter) filter).onCreateDelegationTokenRequest(apiVersion, header,
                    (CreateDelegationTokenRequestData) body, filterContext);
            case CREATE_PARTITIONS -> ((CreatePartitionsRequestFilter) filter).onCreatePartitionsRequest(apiVersion, header, (CreatePartitionsRequestData) body,
                    filterContext);
            case CREATE_TOPICS -> ((CreateTopicsRequestFilter) filter).onCreateTopicsRequest(apiVersion, header, (CreateTopicsRequestData) body, filterContext);
            case DELETE_ACLS -> ((DeleteAclsRequestFilter) filter).onDeleteAclsRequest(apiVersion, header, (DeleteAclsRequestData) body, filterContext);
            case DELETE_GROUPS -> ((DeleteGroupsRequestFilter) filter).onDeleteGroupsRequest(apiVersion, header, (DeleteGroupsRequestData) body, filterContext);
            case DELETE_RECORDS -> ((DeleteRecordsRequestFilter) filter).onDeleteRecordsRequest(apiVersion, header, (DeleteRecordsRequestData) body, filterContext);
            case DELETE_TOPICS -> ((DeleteTopicsRequestFilter) filter).onDeleteTopicsRequest(apiVersion, header, (DeleteTopicsRequestData) body, filterContext);
            case DESCRIBE_ACLS -> ((DescribeAclsRequestFilter) filter).onDescribeAclsRequest(apiVersion, header, (DescribeAclsRequestData) body, filterContext);
            case DESCRIBE_CLIENT_QUOTAS -> ((DescribeClientQuotasRequestFilter) filter).onDescribeClientQuotasRequest(apiVersion, header,
                    (DescribeClientQuotasRequestData) body, filterContext);
            case DESCRIBE_CLUSTER -> ((DescribeClusterRequestFilter) filter).onDescribeClusterRequest(apiVersion, header, (DescribeClusterRequestData) body,
                    filterContext);
            case DESCRIBE_CONFIGS -> ((DescribeConfigsRequestFilter) filter).onDescribeConfigsRequest(apiVersion, header, (DescribeConfigsRequestData) body,
                    filterContext);
            case DESCRIBE_DELEGATION_TOKEN -> ((DescribeDelegationTokenRequestFilter) filter).onDescribeDelegationTokenRequest(apiVersion, header,
                    (DescribeDelegationTokenRequestData) body,
                    filterContext);
            case DESCRIBE_GROUPS -> ((DescribeGroupsRequestFilter) filter).onDescribeGroupsRequest(apiVersion, header, (DescribeGroupsRequestData) body, filterContext);
            case DESCRIBE_LOG_DIRS -> ((DescribeLogDirsRequestFilter) filter).onDescribeLogDirsRequest(apiVersion, header, (DescribeLogDirsRequestData) body,
                    filterContext);
            case DESCRIBE_PRODUCERS -> ((DescribeProducersRequestFilter) filter).onDescribeProducersRequest(apiVersion, header, (DescribeProducersRequestData) body,
                    filterContext);
            case DESCRIBE_QUORUM -> ((DescribeQuorumRequestFilter) filter).onDescribeQuorumRequest(apiVersion, header, (DescribeQuorumRequestData) body, filterContext);
            case DESCRIBE_TRANSACTIONS -> ((DescribeTransactionsRequestFilter) filter).onDescribeTransactionsRequest(apiVersion, header,
                    (DescribeTransactionsRequestData) body, filterContext);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> ((DescribeUserScramCredentialsRequestFilter) filter).onDescribeUserScramCredentialsRequest(apiVersion, header,
                    (DescribeUserScramCredentialsRequestData) body, filterContext);
            case ELECT_LEADERS -> ((ElectLeadersRequestFilter) filter).onElectLeadersRequest(apiVersion, header, (ElectLeadersRequestData) body, filterContext);
            case END_QUORUM_EPOCH -> ((EndQuorumEpochRequestFilter) filter).onEndQuorumEpochRequest(apiVersion, header, (EndQuorumEpochRequestData) body, filterContext);
            case END_TXN -> ((EndTxnRequestFilter) filter).onEndTxnRequest(apiVersion, header, (EndTxnRequestData) body, filterContext);
            case ENVELOPE -> ((EnvelopeRequestFilter) filter).onEnvelopeRequest(apiVersion, header, (EnvelopeRequestData) body, filterContext);
            case EXPIRE_DELEGATION_TOKEN -> ((ExpireDelegationTokenRequestFilter) filter).onExpireDelegationTokenRequest(apiVersion, header,
                    (ExpireDelegationTokenRequestData) body, filterContext);
            case FETCH -> ((FetchRequestFilter) filter).onFetchRequest(apiVersion, header, (FetchRequestData) body, filterContext);
            case FETCH_SNAPSHOT -> ((FetchSnapshotRequestFilter) filter).onFetchSnapshotRequest(apiVersion, header, (FetchSnapshotRequestData) body, filterContext);
            case FIND_COORDINATOR -> ((FindCoordinatorRequestFilter) filter).onFindCoordinatorRequest(apiVersion, header, (FindCoordinatorRequestData) body,
                    filterContext);
            case HEARTBEAT -> ((HeartbeatRequestFilter) filter).onHeartbeatRequest(apiVersion, header, (HeartbeatRequestData) body, filterContext);
            case INCREMENTAL_ALTER_CONFIGS -> ((IncrementalAlterConfigsRequestFilter) filter).onIncrementalAlterConfigsRequest(apiVersion, header,
                    (IncrementalAlterConfigsRequestData) body,
                    filterContext);
            case INIT_PRODUCER_ID -> ((InitProducerIdRequestFilter) filter).onInitProducerIdRequest(apiVersion, header, (InitProducerIdRequestData) body, filterContext);
            case JOIN_GROUP -> ((JoinGroupRequestFilter) filter).onJoinGroupRequest(apiVersion, header, (JoinGroupRequestData) body, filterContext);
            case LEAVE_GROUP -> ((LeaveGroupRequestFilter) filter).onLeaveGroupRequest(apiVersion, header, (LeaveGroupRequestData) body, filterContext);
            case LIST_GROUPS -> ((ListGroupsRequestFilter) filter).onListGroupsRequest(apiVersion, header, (ListGroupsRequestData) body, filterContext);
            case LIST_OFFSETS -> ((ListOffsetsRequestFilter) filter).onListOffsetsRequest(apiVersion, header, (ListOffsetsRequestData) body, filterContext);
            case LIST_PARTITION_REASSIGNMENTS -> ((ListPartitionReassignmentsRequestFilter) filter).onListPartitionReassignmentsRequest(apiVersion, header,
                    (ListPartitionReassignmentsRequestData) body, filterContext);
            case LIST_TRANSACTIONS -> ((ListTransactionsRequestFilter) filter).onListTransactionsRequest(apiVersion, header, (ListTransactionsRequestData) body,
                    filterContext);
            case METADATA -> ((MetadataRequestFilter) filter).onMetadataRequest(apiVersion, header, (MetadataRequestData) body, filterContext);
            case OFFSET_COMMIT -> ((OffsetCommitRequestFilter) filter).onOffsetCommitRequest(apiVersion, header, (OffsetCommitRequestData) body, filterContext);
            case OFFSET_DELETE -> ((OffsetDeleteRequestFilter) filter).onOffsetDeleteRequest(apiVersion, header, (OffsetDeleteRequestData) body, filterContext);
            case OFFSET_FETCH -> ((OffsetFetchRequestFilter) filter).onOffsetFetchRequest(apiVersion, header, (OffsetFetchRequestData) body, filterContext);
            case OFFSET_FOR_LEADER_EPOCH -> ((OffsetForLeaderEpochRequestFilter) filter).onOffsetForLeaderEpochRequest(apiVersion, header,
                    (OffsetForLeaderEpochRequestData) body, filterContext);
            case PRODUCE -> ((ProduceRequestFilter) filter).onProduceRequest(apiVersion, header, (ProduceRequestData) body, filterContext);
            case RENEW_DELEGATION_TOKEN -> ((RenewDelegationTokenRequestFilter) filter).onRenewDelegationTokenRequest(apiVersion, header,
                    (RenewDelegationTokenRequestData) body, filterContext);
            case SASL_AUTHENTICATE -> ((SaslAuthenticateRequestFilter) filter).onSaslAuthenticateRequest(apiVersion, header, (SaslAuthenticateRequestData) body,
                    filterContext);
            case SASL_HANDSHAKE -> ((SaslHandshakeRequestFilter) filter).onSaslHandshakeRequest(apiVersion, header, (SaslHandshakeRequestData) body, filterContext);
            case SYNC_GROUP -> ((SyncGroupRequestFilter) filter).onSyncGroupRequest(apiVersion, header, (SyncGroupRequestData) body, filterContext);
            case TXN_OFFSET_COMMIT -> ((TxnOffsetCommitRequestFilter) filter).onTxnOffsetCommitRequest(apiVersion, header, (TxnOffsetCommitRequestData) body,
                    filterContext);
            case UNREGISTER_BROKER -> ((UnregisterBrokerRequestFilter) filter).onUnregisterBrokerRequest(apiVersion, header, (UnregisterBrokerRequestData) body,
                    filterContext);
            case UPDATE_FEATURES -> ((UpdateFeaturesRequestFilter) filter).onUpdateFeaturesRequest(apiVersion, header, (UpdateFeaturesRequestData) body, filterContext);
            case VOTE -> ((VoteRequestFilter) filter).onVoteRequest(apiVersion, header, (VoteRequestData) body, filterContext);
            case WRITE_TXN_MARKERS -> ((WriteTxnMarkersRequestFilter) filter).onWriteTxnMarkersRequest(apiVersion, header, (WriteTxnMarkersRequestData) body,
                    filterContext);
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        };
    }

    /**
     * Apply the filter to the given {@code header} and {@code body} using the given {@code filterContext}.
     *
     * @param apiKey        The request api key.
     * @param apiVersion    The api version.
     * @param header        The request header.
     * @param body          The request body.
     * @param filterContext The filter context.
     * @return response filter result
     */
    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage body,
                                                            FilterContext filterContext) {
        return switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> ((AddOffsetsToTxnResponseFilter) filter).onAddOffsetsToTxnResponse(apiVersion, header, (AddOffsetsToTxnResponseData) body,
                    filterContext);
            case ADD_PARTITIONS_TO_TXN -> ((AddPartitionsToTxnResponseFilter) filter).onAddPartitionsToTxnResponse(apiVersion, header,
                    (AddPartitionsToTxnResponseData) body, filterContext);
            case ALLOCATE_PRODUCER_IDS -> ((AllocateProducerIdsResponseFilter) filter).onAllocateProducerIdsResponse(apiVersion, header,
                    (AllocateProducerIdsResponseData) body, filterContext);
            case ALTER_CLIENT_QUOTAS -> ((AlterClientQuotasResponseFilter) filter).onAlterClientQuotasResponse(apiVersion, header, (AlterClientQuotasResponseData) body,
                    filterContext);
            case ALTER_CONFIGS -> ((AlterConfigsResponseFilter) filter).onAlterConfigsResponse(apiVersion, header, (AlterConfigsResponseData) body, filterContext);
            case ALTER_PARTITION_REASSIGNMENTS -> ((AlterPartitionReassignmentsResponseFilter) filter).onAlterPartitionReassignmentsResponse(apiVersion, header,
                    (AlterPartitionReassignmentsResponseData) body, filterContext);
            case ALTER_PARTITION -> ((AlterPartitionResponseFilter) filter).onAlterPartitionResponse(apiVersion, header, (AlterPartitionResponseData) body,
                    filterContext);
            case ALTER_REPLICA_LOG_DIRS -> ((AlterReplicaLogDirsResponseFilter) filter).onAlterReplicaLogDirsResponse(apiVersion, header,
                    (AlterReplicaLogDirsResponseData) body, filterContext);
            case ALTER_USER_SCRAM_CREDENTIALS -> ((AlterUserScramCredentialsResponseFilter) filter).onAlterUserScramCredentialsResponse(apiVersion, header,
                    (AlterUserScramCredentialsResponseData) body, filterContext);
            case API_VERSIONS -> ((ApiVersionsResponseFilter) filter).onApiVersionsResponse(apiVersion, header, (ApiVersionsResponseData) body, filterContext);
            case BEGIN_QUORUM_EPOCH -> ((BeginQuorumEpochResponseFilter) filter).onBeginQuorumEpochResponse(apiVersion, header, (BeginQuorumEpochResponseData) body,
                    filterContext);
            case BROKER_HEARTBEAT -> ((BrokerHeartbeatResponseFilter) filter).onBrokerHeartbeatResponse(apiVersion, header, (BrokerHeartbeatResponseData) body,
                    filterContext);
            case BROKER_REGISTRATION -> ((BrokerRegistrationResponseFilter) filter).onBrokerRegistrationResponse(apiVersion, header,
                    (BrokerRegistrationResponseData) body, filterContext);
            case CREATE_ACLS -> ((CreateAclsResponseFilter) filter).onCreateAclsResponse(apiVersion, header, (CreateAclsResponseData) body, filterContext);
            case CREATE_DELEGATION_TOKEN -> ((CreateDelegationTokenResponseFilter) filter).onCreateDelegationTokenResponse(apiVersion, header,
                    (CreateDelegationTokenResponseData) body,
                    filterContext);
            case CREATE_PARTITIONS -> ((CreatePartitionsResponseFilter) filter).onCreatePartitionsResponse(apiVersion, header, (CreatePartitionsResponseData) body,
                    filterContext);
            case CREATE_TOPICS -> ((CreateTopicsResponseFilter) filter).onCreateTopicsResponse(apiVersion, header, (CreateTopicsResponseData) body, filterContext);
            case DELETE_ACLS -> ((DeleteAclsResponseFilter) filter).onDeleteAclsResponse(apiVersion, header, (DeleteAclsResponseData) body, filterContext);
            case DELETE_GROUPS -> ((DeleteGroupsResponseFilter) filter).onDeleteGroupsResponse(apiVersion, header, (DeleteGroupsResponseData) body, filterContext);
            case DELETE_RECORDS -> ((DeleteRecordsResponseFilter) filter).onDeleteRecordsResponse(apiVersion, header, (DeleteRecordsResponseData) body, filterContext);
            case DELETE_TOPICS -> ((DeleteTopicsResponseFilter) filter).onDeleteTopicsResponse(apiVersion, header, (DeleteTopicsResponseData) body, filterContext);
            case DESCRIBE_ACLS -> ((DescribeAclsResponseFilter) filter).onDescribeAclsResponse(apiVersion, header, (DescribeAclsResponseData) body, filterContext);
            case DESCRIBE_CLIENT_QUOTAS -> ((DescribeClientQuotasResponseFilter) filter).onDescribeClientQuotasResponse(apiVersion, header,
                    (DescribeClientQuotasResponseData) body, filterContext);
            case DESCRIBE_CLUSTER -> ((DescribeClusterResponseFilter) filter).onDescribeClusterResponse(apiVersion, header, (DescribeClusterResponseData) body,
                    filterContext);
            case DESCRIBE_CONFIGS -> ((DescribeConfigsResponseFilter) filter).onDescribeConfigsResponse(apiVersion, header, (DescribeConfigsResponseData) body,
                    filterContext);
            case DESCRIBE_DELEGATION_TOKEN -> ((DescribeDelegationTokenResponseFilter) filter).onDescribeDelegationTokenResponse(apiVersion, header,
                    (DescribeDelegationTokenResponseData) body,
                    filterContext);
            case DESCRIBE_GROUPS -> ((DescribeGroupsResponseFilter) filter).onDescribeGroupsResponse(apiVersion, header, (DescribeGroupsResponseData) body,
                    filterContext);
            case DESCRIBE_LOG_DIRS -> ((DescribeLogDirsResponseFilter) filter).onDescribeLogDirsResponse(apiVersion, header, (DescribeLogDirsResponseData) body,
                    filterContext);
            case DESCRIBE_PRODUCERS -> ((DescribeProducersResponseFilter) filter).onDescribeProducersResponse(apiVersion, header, (DescribeProducersResponseData) body,
                    filterContext);
            case DESCRIBE_QUORUM -> ((DescribeQuorumResponseFilter) filter).onDescribeQuorumResponse(apiVersion, header, (DescribeQuorumResponseData) body,
                    filterContext);
            case DESCRIBE_TRANSACTIONS -> ((DescribeTransactionsResponseFilter) filter).onDescribeTransactionsResponse(apiVersion, header,
                    (DescribeTransactionsResponseData) body, filterContext);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> ((DescribeUserScramCredentialsResponseFilter) filter).onDescribeUserScramCredentialsResponse(apiVersion, header,
                    (DescribeUserScramCredentialsResponseData) body, filterContext);
            case ELECT_LEADERS -> ((ElectLeadersResponseFilter) filter).onElectLeadersResponse(apiVersion, header, (ElectLeadersResponseData) body, filterContext);
            case END_QUORUM_EPOCH -> ((EndQuorumEpochResponseFilter) filter).onEndQuorumEpochResponse(apiVersion, header, (EndQuorumEpochResponseData) body,
                    filterContext);
            case END_TXN -> ((EndTxnResponseFilter) filter).onEndTxnResponse(apiVersion, header, (EndTxnResponseData) body, filterContext);
            case ENVELOPE -> ((EnvelopeResponseFilter) filter).onEnvelopeResponse(apiVersion, header, (EnvelopeResponseData) body, filterContext);
            case EXPIRE_DELEGATION_TOKEN -> ((ExpireDelegationTokenResponseFilter) filter).onExpireDelegationTokenResponse(apiVersion, header,
                    (ExpireDelegationTokenResponseData) body,
                    filterContext);
            case FETCH -> ((FetchResponseFilter) filter).onFetchResponse(apiVersion, header, (FetchResponseData) body, filterContext);
            case FETCH_SNAPSHOT -> ((FetchSnapshotResponseFilter) filter).onFetchSnapshotResponse(apiVersion, header, (FetchSnapshotResponseData) body, filterContext);
            case FIND_COORDINATOR -> ((FindCoordinatorResponseFilter) filter).onFindCoordinatorResponse(apiVersion, header, (FindCoordinatorResponseData) body,
                    filterContext);
            case HEARTBEAT -> ((HeartbeatResponseFilter) filter).onHeartbeatResponse(apiVersion, header, (HeartbeatResponseData) body, filterContext);
            case INCREMENTAL_ALTER_CONFIGS -> ((IncrementalAlterConfigsResponseFilter) filter).onIncrementalAlterConfigsResponse(apiVersion, header,
                    (IncrementalAlterConfigsResponseData) body,
                    filterContext);
            case INIT_PRODUCER_ID -> ((InitProducerIdResponseFilter) filter).onInitProducerIdResponse(apiVersion, header, (InitProducerIdResponseData) body,
                    filterContext);
            case JOIN_GROUP -> ((JoinGroupResponseFilter) filter).onJoinGroupResponse(apiVersion, header, (JoinGroupResponseData) body, filterContext);
            case LEAVE_GROUP -> ((LeaveGroupResponseFilter) filter).onLeaveGroupResponse(apiVersion, header, (LeaveGroupResponseData) body, filterContext);
            case LIST_GROUPS -> ((ListGroupsResponseFilter) filter).onListGroupsResponse(apiVersion, header, (ListGroupsResponseData) body, filterContext);
            case LIST_OFFSETS -> ((ListOffsetsResponseFilter) filter).onListOffsetsResponse(apiVersion, header, (ListOffsetsResponseData) body, filterContext);
            case LIST_PARTITION_REASSIGNMENTS -> ((ListPartitionReassignmentsResponseFilter) filter).onListPartitionReassignmentsResponse(apiVersion, header,
                    (ListPartitionReassignmentsResponseData) body, filterContext);
            case LIST_TRANSACTIONS -> ((ListTransactionsResponseFilter) filter).onListTransactionsResponse(apiVersion, header, (ListTransactionsResponseData) body,
                    filterContext);
            case METADATA -> ((MetadataResponseFilter) filter).onMetadataResponse(apiVersion, header, (MetadataResponseData) body, filterContext);
            case OFFSET_COMMIT -> ((OffsetCommitResponseFilter) filter).onOffsetCommitResponse(apiVersion, header, (OffsetCommitResponseData) body, filterContext);
            case OFFSET_DELETE -> ((OffsetDeleteResponseFilter) filter).onOffsetDeleteResponse(apiVersion, header, (OffsetDeleteResponseData) body, filterContext);
            case OFFSET_FETCH -> ((OffsetFetchResponseFilter) filter).onOffsetFetchResponse(apiVersion, header, (OffsetFetchResponseData) body, filterContext);
            case OFFSET_FOR_LEADER_EPOCH -> ((OffsetForLeaderEpochResponseFilter) filter).onOffsetForLeaderEpochResponse(apiVersion, header,
                    (OffsetForLeaderEpochResponseData) body, filterContext);
            case PRODUCE -> ((ProduceResponseFilter) filter).onProduceResponse(apiVersion, header, (ProduceResponseData) body, filterContext);
            case RENEW_DELEGATION_TOKEN -> ((RenewDelegationTokenResponseFilter) filter).onRenewDelegationTokenResponse(apiVersion, header,
                    (RenewDelegationTokenResponseData) body, filterContext);
            case SASL_AUTHENTICATE -> ((SaslAuthenticateResponseFilter) filter).onSaslAuthenticateResponse(apiVersion, header, (SaslAuthenticateResponseData) body,
                    filterContext);
            case SASL_HANDSHAKE -> ((SaslHandshakeResponseFilter) filter).onSaslHandshakeResponse(apiVersion, header, (SaslHandshakeResponseData) body, filterContext);
            case SYNC_GROUP -> ((SyncGroupResponseFilter) filter).onSyncGroupResponse(apiVersion, header, (SyncGroupResponseData) body, filterContext);
            case TXN_OFFSET_COMMIT -> ((TxnOffsetCommitResponseFilter) filter).onTxnOffsetCommitResponse(apiVersion, header, (TxnOffsetCommitResponseData) body,
                    filterContext);
            case UNREGISTER_BROKER -> ((UnregisterBrokerResponseFilter) filter).onUnregisterBrokerResponse(apiVersion, header, (UnregisterBrokerResponseData) body,
                    filterContext);
            case UPDATE_FEATURES -> ((UpdateFeaturesResponseFilter) filter).onUpdateFeaturesResponse(apiVersion, header, (UpdateFeaturesResponseData) body,
                    filterContext);
            case VOTE -> ((VoteResponseFilter) filter).onVoteResponse(apiVersion, header, (VoteResponseData) body, filterContext);
            case WRITE_TXN_MARKERS -> ((WriteTxnMarkersResponseFilter) filter).onWriteTxnMarkersResponse(apiVersion, header, (WriteTxnMarkersResponseData) body,
                    filterContext);
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        };
    }

    /**
     * <p>Determines whether a request with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * Note that it is not guaranteed that this method will be called once per request,
     * or that two consecutive calls refer to the same request.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldHandleRequest} on request A</li>
     *     <li>{@code shouldHandleRequest} on request B</li>
     *     <li>{@code shouldHandleRequest} on request A</li>
     *     <li>{@code onRequest} on request A</li>
     *     <li>{@code onRequest} on request B</li>
     * </ol>
     * @param apiKey The API key
     * @param apiVersion The API version
     * @return true if request should be deserialized
     */
    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> filter instanceof AddOffsetsToTxnRequestFilter
                    && ((AddOffsetsToTxnRequestFilter) filter).shouldHandleAddOffsetsToTxnRequest(apiVersion);
            case ADD_PARTITIONS_TO_TXN -> filter instanceof AddPartitionsToTxnRequestFilter
                    && ((AddPartitionsToTxnRequestFilter) filter).shouldHandleAddPartitionsToTxnRequest(apiVersion);
            case ALLOCATE_PRODUCER_IDS -> filter instanceof AllocateProducerIdsRequestFilter
                    && ((AllocateProducerIdsRequestFilter) filter).shouldHandleAllocateProducerIdsRequest(apiVersion);
            case ALTER_CLIENT_QUOTAS -> filter instanceof AlterClientQuotasRequestFilter
                    && ((AlterClientQuotasRequestFilter) filter).shouldHandleAlterClientQuotasRequest(apiVersion);
            case ALTER_CONFIGS -> filter instanceof AlterConfigsRequestFilter && ((AlterConfigsRequestFilter) filter).shouldHandleAlterConfigsRequest(apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> filter instanceof AlterPartitionReassignmentsRequestFilter
                    && ((AlterPartitionReassignmentsRequestFilter) filter).shouldHandleAlterPartitionReassignmentsRequest(apiVersion);
            case ALTER_PARTITION -> filter instanceof AlterPartitionRequestFilter && ((AlterPartitionRequestFilter) filter).shouldHandleAlterPartitionRequest(apiVersion);
            case ALTER_REPLICA_LOG_DIRS -> filter instanceof AlterReplicaLogDirsRequestFilter
                    && ((AlterReplicaLogDirsRequestFilter) filter).shouldHandleAlterReplicaLogDirsRequest(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> filter instanceof AlterUserScramCredentialsRequestFilter
                    && ((AlterUserScramCredentialsRequestFilter) filter).shouldHandleAlterUserScramCredentialsRequest(apiVersion);
            case API_VERSIONS -> filter instanceof ApiVersionsRequestFilter && ((ApiVersionsRequestFilter) filter).shouldHandleApiVersionsRequest(apiVersion);
            case BEGIN_QUORUM_EPOCH -> filter instanceof BeginQuorumEpochRequestFilter
                    && ((BeginQuorumEpochRequestFilter) filter).shouldHandleBeginQuorumEpochRequest(apiVersion);
            case BROKER_HEARTBEAT -> filter instanceof BrokerHeartbeatRequestFilter
                    && ((BrokerHeartbeatRequestFilter) filter).shouldHandleBrokerHeartbeatRequest(apiVersion);
            case BROKER_REGISTRATION -> filter instanceof BrokerRegistrationRequestFilter
                    && ((BrokerRegistrationRequestFilter) filter).shouldHandleBrokerRegistrationRequest(apiVersion);
            case CREATE_ACLS -> filter instanceof CreateAclsRequestFilter && ((CreateAclsRequestFilter) filter).shouldHandleCreateAclsRequest(apiVersion);
            case CREATE_DELEGATION_TOKEN -> filter instanceof CreateDelegationTokenRequestFilter
                    && ((CreateDelegationTokenRequestFilter) filter).shouldHandleCreateDelegationTokenRequest(apiVersion);
            case CREATE_PARTITIONS -> filter instanceof CreatePartitionsRequestFilter
                    && ((CreatePartitionsRequestFilter) filter).shouldHandleCreatePartitionsRequest(apiVersion);
            case CREATE_TOPICS -> filter instanceof CreateTopicsRequestFilter && ((CreateTopicsRequestFilter) filter).shouldHandleCreateTopicsRequest(apiVersion);
            case DELETE_ACLS -> filter instanceof DeleteAclsRequestFilter && ((DeleteAclsRequestFilter) filter).shouldHandleDeleteAclsRequest(apiVersion);
            case DELETE_GROUPS -> filter instanceof DeleteGroupsRequestFilter && ((DeleteGroupsRequestFilter) filter).shouldHandleDeleteGroupsRequest(apiVersion);
            case DELETE_RECORDS -> filter instanceof DeleteRecordsRequestFilter && ((DeleteRecordsRequestFilter) filter).shouldHandleDeleteRecordsRequest(apiVersion);
            case DELETE_TOPICS -> filter instanceof DeleteTopicsRequestFilter && ((DeleteTopicsRequestFilter) filter).shouldHandleDeleteTopicsRequest(apiVersion);
            case DESCRIBE_ACLS -> filter instanceof DescribeAclsRequestFilter && ((DescribeAclsRequestFilter) filter).shouldHandleDescribeAclsRequest(apiVersion);
            case DESCRIBE_CLIENT_QUOTAS -> filter instanceof DescribeClientQuotasRequestFilter
                    && ((DescribeClientQuotasRequestFilter) filter).shouldHandleDescribeClientQuotasRequest(apiVersion);
            case DESCRIBE_CLUSTER -> filter instanceof DescribeClusterRequestFilter
                    && ((DescribeClusterRequestFilter) filter).shouldHandleDescribeClusterRequest(apiVersion);
            case DESCRIBE_CONFIGS -> filter instanceof DescribeConfigsRequestFilter
                    && ((DescribeConfigsRequestFilter) filter).shouldHandleDescribeConfigsRequest(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> filter instanceof DescribeDelegationTokenRequestFilter
                    && ((DescribeDelegationTokenRequestFilter) filter).shouldHandleDescribeDelegationTokenRequest(apiVersion);
            case DESCRIBE_GROUPS -> filter instanceof DescribeGroupsRequestFilter && ((DescribeGroupsRequestFilter) filter).shouldHandleDescribeGroupsRequest(apiVersion);
            case DESCRIBE_LOG_DIRS -> filter instanceof DescribeLogDirsRequestFilter
                    && ((DescribeLogDirsRequestFilter) filter).shouldHandleDescribeLogDirsRequest(apiVersion);
            case DESCRIBE_PRODUCERS -> filter instanceof DescribeProducersRequestFilter
                    && ((DescribeProducersRequestFilter) filter).shouldHandleDescribeProducersRequest(apiVersion);
            case DESCRIBE_QUORUM -> filter instanceof DescribeQuorumRequestFilter && ((DescribeQuorumRequestFilter) filter).shouldHandleDescribeQuorumRequest(apiVersion);
            case DESCRIBE_TRANSACTIONS -> filter instanceof DescribeTransactionsRequestFilter
                    && ((DescribeTransactionsRequestFilter) filter).shouldHandleDescribeTransactionsRequest(apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> filter instanceof DescribeUserScramCredentialsRequestFilter
                    && ((DescribeUserScramCredentialsRequestFilter) filter).shouldHandleDescribeUserScramCredentialsRequest(apiVersion);
            case ELECT_LEADERS -> filter instanceof ElectLeadersRequestFilter && ((ElectLeadersRequestFilter) filter).shouldHandleElectLeadersRequest(apiVersion);
            case END_QUORUM_EPOCH -> filter instanceof EndQuorumEpochRequestFilter
                    && ((EndQuorumEpochRequestFilter) filter).shouldHandleEndQuorumEpochRequest(apiVersion);
            case END_TXN -> filter instanceof EndTxnRequestFilter && ((EndTxnRequestFilter) filter).shouldHandleEndTxnRequest(apiVersion);
            case ENVELOPE -> filter instanceof EnvelopeRequestFilter && ((EnvelopeRequestFilter) filter).shouldHandleEnvelopeRequest(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> filter instanceof ExpireDelegationTokenRequestFilter
                    && ((ExpireDelegationTokenRequestFilter) filter).shouldHandleExpireDelegationTokenRequest(apiVersion);
            case FETCH -> filter instanceof FetchRequestFilter && ((FetchRequestFilter) filter).shouldHandleFetchRequest(apiVersion);
            case FETCH_SNAPSHOT -> filter instanceof FetchSnapshotRequestFilter && ((FetchSnapshotRequestFilter) filter).shouldHandleFetchSnapshotRequest(apiVersion);
            case FIND_COORDINATOR -> filter instanceof FindCoordinatorRequestFilter
                    && ((FindCoordinatorRequestFilter) filter).shouldHandleFindCoordinatorRequest(apiVersion);
            case HEARTBEAT -> filter instanceof HeartbeatRequestFilter && ((HeartbeatRequestFilter) filter).shouldHandleHeartbeatRequest(apiVersion);
            case INCREMENTAL_ALTER_CONFIGS -> filter instanceof IncrementalAlterConfigsRequestFilter
                    && ((IncrementalAlterConfigsRequestFilter) filter).shouldHandleIncrementalAlterConfigsRequest(apiVersion);
            case INIT_PRODUCER_ID -> filter instanceof InitProducerIdRequestFilter
                    && ((InitProducerIdRequestFilter) filter).shouldHandleInitProducerIdRequest(apiVersion);
            case JOIN_GROUP -> filter instanceof JoinGroupRequestFilter && ((JoinGroupRequestFilter) filter).shouldHandleJoinGroupRequest(apiVersion);
            case LEAVE_GROUP -> filter instanceof LeaveGroupRequestFilter && ((LeaveGroupRequestFilter) filter).shouldHandleLeaveGroupRequest(apiVersion);
            case LIST_GROUPS -> filter instanceof ListGroupsRequestFilter && ((ListGroupsRequestFilter) filter).shouldHandleListGroupsRequest(apiVersion);
            case LIST_OFFSETS -> filter instanceof ListOffsetsRequestFilter && ((ListOffsetsRequestFilter) filter).shouldHandleListOffsetsRequest(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> filter instanceof ListPartitionReassignmentsRequestFilter
                    && ((ListPartitionReassignmentsRequestFilter) filter).shouldHandleListPartitionReassignmentsRequest(apiVersion);
            case LIST_TRANSACTIONS -> filter instanceof ListTransactionsRequestFilter
                    && ((ListTransactionsRequestFilter) filter).shouldHandleListTransactionsRequest(apiVersion);
            case METADATA -> filter instanceof MetadataRequestFilter && ((MetadataRequestFilter) filter).shouldHandleMetadataRequest(apiVersion);
            case OFFSET_COMMIT -> filter instanceof OffsetCommitRequestFilter && ((OffsetCommitRequestFilter) filter).shouldHandleOffsetCommitRequest(apiVersion);
            case OFFSET_DELETE -> filter instanceof OffsetDeleteRequestFilter && ((OffsetDeleteRequestFilter) filter).shouldHandleOffsetDeleteRequest(apiVersion);
            case OFFSET_FETCH -> filter instanceof OffsetFetchRequestFilter && ((OffsetFetchRequestFilter) filter).shouldHandleOffsetFetchRequest(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH -> filter instanceof OffsetForLeaderEpochRequestFilter
                    && ((OffsetForLeaderEpochRequestFilter) filter).shouldHandleOffsetForLeaderEpochRequest(apiVersion);
            case PRODUCE -> filter instanceof ProduceRequestFilter && ((ProduceRequestFilter) filter).shouldHandleProduceRequest(apiVersion);
            case RENEW_DELEGATION_TOKEN -> filter instanceof RenewDelegationTokenRequestFilter
                    && ((RenewDelegationTokenRequestFilter) filter).shouldHandleRenewDelegationTokenRequest(apiVersion);
            case SASL_AUTHENTICATE -> filter instanceof SaslAuthenticateRequestFilter
                    && ((SaslAuthenticateRequestFilter) filter).shouldHandleSaslAuthenticateRequest(apiVersion);
            case SASL_HANDSHAKE -> filter instanceof SaslHandshakeRequestFilter && ((SaslHandshakeRequestFilter) filter).shouldHandleSaslHandshakeRequest(apiVersion);
            case SYNC_GROUP -> filter instanceof SyncGroupRequestFilter && ((SyncGroupRequestFilter) filter).shouldHandleSyncGroupRequest(apiVersion);
            case TXN_OFFSET_COMMIT -> filter instanceof TxnOffsetCommitRequestFilter
                    && ((TxnOffsetCommitRequestFilter) filter).shouldHandleTxnOffsetCommitRequest(apiVersion);
            case UNREGISTER_BROKER -> filter instanceof UnregisterBrokerRequestFilter
                    && ((UnregisterBrokerRequestFilter) filter).shouldHandleUnregisterBrokerRequest(apiVersion);
            case UPDATE_FEATURES -> filter instanceof UpdateFeaturesRequestFilter && ((UpdateFeaturesRequestFilter) filter).shouldHandleUpdateFeaturesRequest(apiVersion);
            case VOTE -> filter instanceof VoteRequestFilter && ((VoteRequestFilter) filter).shouldHandleVoteRequest(apiVersion);
            case WRITE_TXN_MARKERS -> filter instanceof WriteTxnMarkersRequestFilter
                    && ((WriteTxnMarkersRequestFilter) filter).shouldHandleWriteTxnMarkersRequest(apiVersion);
            default -> throw new IllegalStateException("Unsupported API key " + apiKey);
        };
    }

    /**
     * <p>Determines whether a response with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * Note that it is not guaranteed that this method will be called once per response,
     * or that two consecutive calls refer to the same response.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldHandleResponse} on response A</li>
     *     <li>{@code shouldHandleResponse} on response B</li>
     *     <li>{@code shouldHandleResponse} on response A</li>
     *     <li>{@code apply} on response A</li>
     *     <li>{@code apply} on response B</li>
     * </ol>
     * @param apiKey The API key
     * @param apiVersion The API version
     * @return true if response should be deserialized
     */
    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> filter instanceof AddOffsetsToTxnResponseFilter
                    && ((AddOffsetsToTxnResponseFilter) filter).shouldHandleAddOffsetsToTxnResponse(apiVersion);
            case ADD_PARTITIONS_TO_TXN -> filter instanceof AddPartitionsToTxnResponseFilter
                    && ((AddPartitionsToTxnResponseFilter) filter).shouldHandleAddPartitionsToTxnResponse(apiVersion);
            case ALLOCATE_PRODUCER_IDS -> filter instanceof AllocateProducerIdsResponseFilter
                    && ((AllocateProducerIdsResponseFilter) filter).shouldHandleAllocateProducerIdsResponse(apiVersion);
            case ALTER_CLIENT_QUOTAS -> filter instanceof AlterClientQuotasResponseFilter
                    && ((AlterClientQuotasResponseFilter) filter).shouldHandleAlterClientQuotasResponse(apiVersion);
            case ALTER_CONFIGS -> filter instanceof AlterConfigsResponseFilter && ((AlterConfigsResponseFilter) filter).shouldHandleAlterConfigsResponse(apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> filter instanceof AlterPartitionReassignmentsResponseFilter
                    && ((AlterPartitionReassignmentsResponseFilter) filter).shouldHandleAlterPartitionReassignmentsResponse(apiVersion);
            case ALTER_PARTITION -> filter instanceof AlterPartitionResponseFilter
                    && ((AlterPartitionResponseFilter) filter).shouldHandleAlterPartitionResponse(apiVersion);
            case ALTER_REPLICA_LOG_DIRS -> filter instanceof AlterReplicaLogDirsResponseFilter
                    && ((AlterReplicaLogDirsResponseFilter) filter).shouldHandleAlterReplicaLogDirsResponse(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> filter instanceof AlterUserScramCredentialsResponseFilter
                    && ((AlterUserScramCredentialsResponseFilter) filter).shouldHandleAlterUserScramCredentialsResponse(apiVersion);
            case API_VERSIONS -> filter instanceof ApiVersionsResponseFilter && ((ApiVersionsResponseFilter) filter).shouldHandleApiVersionsResponse(apiVersion);
            case BEGIN_QUORUM_EPOCH -> filter instanceof BeginQuorumEpochResponseFilter
                    && ((BeginQuorumEpochResponseFilter) filter).shouldHandleBeginQuorumEpochResponse(apiVersion);
            case BROKER_HEARTBEAT -> filter instanceof BrokerHeartbeatResponseFilter
                    && ((BrokerHeartbeatResponseFilter) filter).shouldHandleBrokerHeartbeatResponse(apiVersion);
            case BROKER_REGISTRATION -> filter instanceof BrokerRegistrationResponseFilter
                    && ((BrokerRegistrationResponseFilter) filter).shouldHandleBrokerRegistrationResponse(apiVersion);
            case CREATE_ACLS -> filter instanceof CreateAclsResponseFilter && ((CreateAclsResponseFilter) filter).shouldHandleCreateAclsResponse(apiVersion);
            case CREATE_DELEGATION_TOKEN -> filter instanceof CreateDelegationTokenResponseFilter
                    && ((CreateDelegationTokenResponseFilter) filter).shouldHandleCreateDelegationTokenResponse(apiVersion);
            case CREATE_PARTITIONS -> filter instanceof CreatePartitionsResponseFilter
                    && ((CreatePartitionsResponseFilter) filter).shouldHandleCreatePartitionsResponse(apiVersion);
            case CREATE_TOPICS -> filter instanceof CreateTopicsResponseFilter && ((CreateTopicsResponseFilter) filter).shouldHandleCreateTopicsResponse(apiVersion);
            case DELETE_ACLS -> filter instanceof DeleteAclsResponseFilter && ((DeleteAclsResponseFilter) filter).shouldHandleDeleteAclsResponse(apiVersion);
            case DELETE_GROUPS -> filter instanceof DeleteGroupsResponseFilter && ((DeleteGroupsResponseFilter) filter).shouldHandleDeleteGroupsResponse(apiVersion);
            case DELETE_RECORDS -> filter instanceof DeleteRecordsResponseFilter && ((DeleteRecordsResponseFilter) filter).shouldHandleDeleteRecordsResponse(apiVersion);
            case DELETE_TOPICS -> filter instanceof DeleteTopicsResponseFilter && ((DeleteTopicsResponseFilter) filter).shouldHandleDeleteTopicsResponse(apiVersion);
            case DESCRIBE_ACLS -> filter instanceof DescribeAclsResponseFilter && ((DescribeAclsResponseFilter) filter).shouldHandleDescribeAclsResponse(apiVersion);
            case DESCRIBE_CLIENT_QUOTAS -> filter instanceof DescribeClientQuotasResponseFilter
                    && ((DescribeClientQuotasResponseFilter) filter).shouldHandleDescribeClientQuotasResponse(apiVersion);
            case DESCRIBE_CLUSTER -> filter instanceof DescribeClusterResponseFilter
                    && ((DescribeClusterResponseFilter) filter).shouldHandleDescribeClusterResponse(apiVersion);
            case DESCRIBE_CONFIGS -> filter instanceof DescribeConfigsResponseFilter
                    && ((DescribeConfigsResponseFilter) filter).shouldHandleDescribeConfigsResponse(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> filter instanceof DescribeDelegationTokenResponseFilter
                    && ((DescribeDelegationTokenResponseFilter) filter).shouldHandleDescribeDelegationTokenResponse(apiVersion);
            case DESCRIBE_GROUPS -> filter instanceof DescribeGroupsResponseFilter
                    && ((DescribeGroupsResponseFilter) filter).shouldHandleDescribeGroupsResponse(apiVersion);
            case DESCRIBE_LOG_DIRS -> filter instanceof DescribeLogDirsResponseFilter
                    && ((DescribeLogDirsResponseFilter) filter).shouldHandleDescribeLogDirsResponse(apiVersion);
            case DESCRIBE_PRODUCERS -> filter instanceof DescribeProducersResponseFilter
                    && ((DescribeProducersResponseFilter) filter).shouldHandleDescribeProducersResponse(apiVersion);
            case DESCRIBE_QUORUM -> filter instanceof DescribeQuorumResponseFilter
                    && ((DescribeQuorumResponseFilter) filter).shouldHandleDescribeQuorumResponse(apiVersion);
            case DESCRIBE_TRANSACTIONS -> filter instanceof DescribeTransactionsResponseFilter
                    && ((DescribeTransactionsResponseFilter) filter).shouldHandleDescribeTransactionsResponse(apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> filter instanceof DescribeUserScramCredentialsResponseFilter
                    && ((DescribeUserScramCredentialsResponseFilter) filter).shouldHandleDescribeUserScramCredentialsResponse(apiVersion);
            case ELECT_LEADERS -> filter instanceof ElectLeadersResponseFilter && ((ElectLeadersResponseFilter) filter).shouldHandleElectLeadersResponse(apiVersion);
            case END_QUORUM_EPOCH -> filter instanceof EndQuorumEpochResponseFilter
                    && ((EndQuorumEpochResponseFilter) filter).shouldHandleEndQuorumEpochResponse(apiVersion);
            case END_TXN -> filter instanceof EndTxnResponseFilter && ((EndTxnResponseFilter) filter).shouldHandleEndTxnResponse(apiVersion);
            case ENVELOPE -> filter instanceof EnvelopeResponseFilter && ((EnvelopeResponseFilter) filter).shouldHandleEnvelopeResponse(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> filter instanceof ExpireDelegationTokenResponseFilter
                    && ((ExpireDelegationTokenResponseFilter) filter).shouldHandleExpireDelegationTokenResponse(apiVersion);
            case FETCH -> filter instanceof FetchResponseFilter && ((FetchResponseFilter) filter).shouldHandleFetchResponse(apiVersion);
            case FETCH_SNAPSHOT -> filter instanceof FetchSnapshotResponseFilter && ((FetchSnapshotResponseFilter) filter).shouldHandleFetchSnapshotResponse(apiVersion);
            case FIND_COORDINATOR -> filter instanceof FindCoordinatorResponseFilter
                    && ((FindCoordinatorResponseFilter) filter).shouldHandleFindCoordinatorResponse(apiVersion);
            case HEARTBEAT -> filter instanceof HeartbeatResponseFilter && ((HeartbeatResponseFilter) filter).shouldHandleHeartbeatResponse(apiVersion);
            case INCREMENTAL_ALTER_CONFIGS -> filter instanceof IncrementalAlterConfigsResponseFilter
                    && ((IncrementalAlterConfigsResponseFilter) filter).shouldHandleIncrementalAlterConfigsResponse(apiVersion);
            case INIT_PRODUCER_ID -> filter instanceof InitProducerIdResponseFilter
                    && ((InitProducerIdResponseFilter) filter).shouldHandleInitProducerIdResponse(apiVersion);
            case JOIN_GROUP -> filter instanceof JoinGroupResponseFilter && ((JoinGroupResponseFilter) filter).shouldHandleJoinGroupResponse(apiVersion);
            case LEAVE_GROUP -> filter instanceof LeaveGroupResponseFilter && ((LeaveGroupResponseFilter) filter).shouldHandleLeaveGroupResponse(apiVersion);
            case LIST_GROUPS -> filter instanceof ListGroupsResponseFilter && ((ListGroupsResponseFilter) filter).shouldHandleListGroupsResponse(apiVersion);
            case LIST_OFFSETS -> filter instanceof ListOffsetsResponseFilter && ((ListOffsetsResponseFilter) filter).shouldHandleListOffsetsResponse(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> filter instanceof ListPartitionReassignmentsResponseFilter
                    && ((ListPartitionReassignmentsResponseFilter) filter).shouldHandleListPartitionReassignmentsResponse(apiVersion);
            case LIST_TRANSACTIONS -> filter instanceof ListTransactionsResponseFilter
                    && ((ListTransactionsResponseFilter) filter).shouldHandleListTransactionsResponse(apiVersion);
            case METADATA -> filter instanceof MetadataResponseFilter && ((MetadataResponseFilter) filter).shouldHandleMetadataResponse(apiVersion);
            case OFFSET_COMMIT -> filter instanceof OffsetCommitResponseFilter && ((OffsetCommitResponseFilter) filter).shouldHandleOffsetCommitResponse(apiVersion);
            case OFFSET_DELETE -> filter instanceof OffsetDeleteResponseFilter && ((OffsetDeleteResponseFilter) filter).shouldHandleOffsetDeleteResponse(apiVersion);
            case OFFSET_FETCH -> filter instanceof OffsetFetchResponseFilter && ((OffsetFetchResponseFilter) filter).shouldHandleOffsetFetchResponse(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH -> filter instanceof OffsetForLeaderEpochResponseFilter
                    && ((OffsetForLeaderEpochResponseFilter) filter).shouldHandleOffsetForLeaderEpochResponse(apiVersion);
            case PRODUCE -> filter instanceof ProduceResponseFilter && ((ProduceResponseFilter) filter).shouldHandleProduceResponse(apiVersion);
            case RENEW_DELEGATION_TOKEN -> filter instanceof RenewDelegationTokenResponseFilter
                    && ((RenewDelegationTokenResponseFilter) filter).shouldHandleRenewDelegationTokenResponse(apiVersion);
            case SASL_AUTHENTICATE -> filter instanceof SaslAuthenticateResponseFilter
                    && ((SaslAuthenticateResponseFilter) filter).shouldHandleSaslAuthenticateResponse(apiVersion);
            case SASL_HANDSHAKE -> filter instanceof SaslHandshakeResponseFilter && ((SaslHandshakeResponseFilter) filter).shouldHandleSaslHandshakeResponse(apiVersion);
            case SYNC_GROUP -> filter instanceof SyncGroupResponseFilter && ((SyncGroupResponseFilter) filter).shouldHandleSyncGroupResponse(apiVersion);
            case TXN_OFFSET_COMMIT -> filter instanceof TxnOffsetCommitResponseFilter
                    && ((TxnOffsetCommitResponseFilter) filter).shouldHandleTxnOffsetCommitResponse(apiVersion);
            case UNREGISTER_BROKER -> filter instanceof UnregisterBrokerResponseFilter
                    && ((UnregisterBrokerResponseFilter) filter).shouldHandleUnregisterBrokerResponse(apiVersion);
            case UPDATE_FEATURES -> filter instanceof UpdateFeaturesResponseFilter
                    && ((UpdateFeaturesResponseFilter) filter).shouldHandleUpdateFeaturesResponse(apiVersion);
            case VOTE -> filter instanceof VoteResponseFilter && ((VoteResponseFilter) filter).shouldHandleVoteResponse(apiVersion);
            case WRITE_TXN_MARKERS -> filter instanceof WriteTxnMarkersResponseFilter
                    && ((WriteTxnMarkersResponseFilter) filter).shouldHandleWriteTxnMarkersResponse(apiVersion);
            default -> throw new IllegalStateException("Unsupported API key " + apiKey);
        };
    }
}
