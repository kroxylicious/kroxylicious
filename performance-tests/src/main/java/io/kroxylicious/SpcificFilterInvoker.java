/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

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
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
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
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
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
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.AddOffsetsToTxnRequestFilter;
import io.kroxylicious.proxy.filter.AddOffsetsToTxnResponseFilter;
import io.kroxylicious.proxy.filter.AddPartitionsToTxnRequestFilter;
import io.kroxylicious.proxy.filter.AddPartitionsToTxnResponseFilter;
import io.kroxylicious.proxy.filter.AllocateProducerIdsRequestFilter;
import io.kroxylicious.proxy.filter.AllocateProducerIdsResponseFilter;
import io.kroxylicious.proxy.filter.AlterClientQuotasRequestFilter;
import io.kroxylicious.proxy.filter.AlterClientQuotasResponseFilter;
import io.kroxylicious.proxy.filter.AlterConfigsRequestFilter;
import io.kroxylicious.proxy.filter.AlterConfigsResponseFilter;
import io.kroxylicious.proxy.filter.AlterPartitionReassignmentsRequestFilter;
import io.kroxylicious.proxy.filter.AlterPartitionReassignmentsResponseFilter;
import io.kroxylicious.proxy.filter.AlterPartitionRequestFilter;
import io.kroxylicious.proxy.filter.AlterPartitionResponseFilter;
import io.kroxylicious.proxy.filter.AlterReplicaLogDirsRequestFilter;
import io.kroxylicious.proxy.filter.AlterReplicaLogDirsResponseFilter;
import io.kroxylicious.proxy.filter.AlterUserScramCredentialsRequestFilter;
import io.kroxylicious.proxy.filter.AlterUserScramCredentialsResponseFilter;
import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.BeginQuorumEpochRequestFilter;
import io.kroxylicious.proxy.filter.BeginQuorumEpochResponseFilter;
import io.kroxylicious.proxy.filter.BrokerHeartbeatRequestFilter;
import io.kroxylicious.proxy.filter.BrokerHeartbeatResponseFilter;
import io.kroxylicious.proxy.filter.BrokerRegistrationRequestFilter;
import io.kroxylicious.proxy.filter.BrokerRegistrationResponseFilter;
import io.kroxylicious.proxy.filter.ControlledShutdownRequestFilter;
import io.kroxylicious.proxy.filter.ControlledShutdownResponseFilter;
import io.kroxylicious.proxy.filter.CreateAclsRequestFilter;
import io.kroxylicious.proxy.filter.CreateAclsResponseFilter;
import io.kroxylicious.proxy.filter.CreateDelegationTokenRequestFilter;
import io.kroxylicious.proxy.filter.CreateDelegationTokenResponseFilter;
import io.kroxylicious.proxy.filter.CreatePartitionsRequestFilter;
import io.kroxylicious.proxy.filter.CreatePartitionsResponseFilter;
import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteAclsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteAclsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteGroupsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteGroupsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteRecordsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteRecordsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeAclsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeAclsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeClientQuotasRequestFilter;
import io.kroxylicious.proxy.filter.DescribeClientQuotasResponseFilter;
import io.kroxylicious.proxy.filter.DescribeClusterRequestFilter;
import io.kroxylicious.proxy.filter.DescribeClusterResponseFilter;
import io.kroxylicious.proxy.filter.DescribeConfigsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeConfigsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeDelegationTokenRequestFilter;
import io.kroxylicious.proxy.filter.DescribeDelegationTokenResponseFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeLogDirsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeLogDirsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeProducersRequestFilter;
import io.kroxylicious.proxy.filter.DescribeProducersResponseFilter;
import io.kroxylicious.proxy.filter.DescribeQuorumRequestFilter;
import io.kroxylicious.proxy.filter.DescribeQuorumResponseFilter;
import io.kroxylicious.proxy.filter.DescribeTransactionsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeTransactionsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeUserScramCredentialsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeUserScramCredentialsResponseFilter;
import io.kroxylicious.proxy.filter.ElectLeadersRequestFilter;
import io.kroxylicious.proxy.filter.ElectLeadersResponseFilter;
import io.kroxylicious.proxy.filter.EndQuorumEpochRequestFilter;
import io.kroxylicious.proxy.filter.EndQuorumEpochResponseFilter;
import io.kroxylicious.proxy.filter.EndTxnRequestFilter;
import io.kroxylicious.proxy.filter.EndTxnResponseFilter;
import io.kroxylicious.proxy.filter.EnvelopeRequestFilter;
import io.kroxylicious.proxy.filter.EnvelopeResponseFilter;
import io.kroxylicious.proxy.filter.ExpireDelegationTokenRequestFilter;
import io.kroxylicious.proxy.filter.ExpireDelegationTokenResponseFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FetchSnapshotRequestFilter;
import io.kroxylicious.proxy.filter.FetchSnapshotResponseFilter;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FindCoordinatorRequestFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.HeartbeatRequestFilter;
import io.kroxylicious.proxy.filter.HeartbeatResponseFilter;
import io.kroxylicious.proxy.filter.IncrementalAlterConfigsRequestFilter;
import io.kroxylicious.proxy.filter.IncrementalAlterConfigsResponseFilter;
import io.kroxylicious.proxy.filter.InitProducerIdRequestFilter;
import io.kroxylicious.proxy.filter.InitProducerIdResponseFilter;
import io.kroxylicious.proxy.filter.JoinGroupRequestFilter;
import io.kroxylicious.proxy.filter.JoinGroupResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.LeaderAndIsrRequestFilter;
import io.kroxylicious.proxy.filter.LeaderAndIsrResponseFilter;
import io.kroxylicious.proxy.filter.LeaveGroupRequestFilter;
import io.kroxylicious.proxy.filter.LeaveGroupResponseFilter;
import io.kroxylicious.proxy.filter.ListGroupsRequestFilter;
import io.kroxylicious.proxy.filter.ListGroupsResponseFilter;
import io.kroxylicious.proxy.filter.ListOffsetsRequestFilter;
import io.kroxylicious.proxy.filter.ListOffsetsResponseFilter;
import io.kroxylicious.proxy.filter.ListPartitionReassignmentsRequestFilter;
import io.kroxylicious.proxy.filter.ListPartitionReassignmentsResponseFilter;
import io.kroxylicious.proxy.filter.ListTransactionsRequestFilter;
import io.kroxylicious.proxy.filter.ListTransactionsResponseFilter;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.OffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteRequestFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteResponseFilter;
import io.kroxylicious.proxy.filter.OffsetFetchRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchResponseFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochRequestFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RenewDelegationTokenRequestFilter;
import io.kroxylicious.proxy.filter.RenewDelegationTokenResponseFilter;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.filter.SaslAuthenticateResponseFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeRequestFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeResponseFilter;
import io.kroxylicious.proxy.filter.StopReplicaRequestFilter;
import io.kroxylicious.proxy.filter.StopReplicaResponseFilter;
import io.kroxylicious.proxy.filter.SyncGroupRequestFilter;
import io.kroxylicious.proxy.filter.SyncGroupResponseFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.UnregisterBrokerRequestFilter;
import io.kroxylicious.proxy.filter.UnregisterBrokerResponseFilter;
import io.kroxylicious.proxy.filter.UpdateFeaturesRequestFilter;
import io.kroxylicious.proxy.filter.UpdateFeaturesResponseFilter;
import io.kroxylicious.proxy.filter.UpdateMetadataRequestFilter;
import io.kroxylicious.proxy.filter.UpdateMetadataResponseFilter;
import io.kroxylicious.proxy.filter.VoteRequestFilter;
import io.kroxylicious.proxy.filter.VoteResponseFilter;
import io.kroxylicious.proxy.filter.WriteTxnMarkersRequestFilter;
import io.kroxylicious.proxy.filter.WriteTxnMarkersResponseFilter;

/**
 * Invoker for KrpcFilters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
class SpecificFilterInvoker implements FilterInvoker {

    private final KrpcFilter filter;

    SpecificFilterInvoker(KrpcFilter filter) {
        this.filter = filter;
    }

    /**
     * Apply the filter to the given {@code header} and {@code body} using the given {@code filterContext}.
     * @param apiKey The request api key.
     * @param apiVersion The request api version.
     * @param header The request header.
     * @param body The request body.
     * @param filterContext The filter context.
     */
    @Override
    public void onRequest(ApiKeys apiKey,
                          short apiVersion,
                          RequestHeaderData header,
                          ApiMessage body,
                          KrpcFilterContext filterContext) {
        switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> ((AddOffsetsToTxnRequestFilter) filter).onAddOffsetsToTxnRequest(header, (AddOffsetsToTxnRequestData) body, filterContext);
            case ADD_PARTITIONS_TO_TXN ->
                ((AddPartitionsToTxnRequestFilter) filter).onAddPartitionsToTxnRequest(header, (AddPartitionsToTxnRequestData) body, filterContext);
            case ALLOCATE_PRODUCER_IDS ->
                ((AllocateProducerIdsRequestFilter) filter).onAllocateProducerIdsRequest(header, (AllocateProducerIdsRequestData) body, filterContext);
            case ALTER_CLIENT_QUOTAS -> ((AlterClientQuotasRequestFilter) filter).onAlterClientQuotasRequest(header, (AlterClientQuotasRequestData) body, filterContext);
            case ALTER_CONFIGS -> ((AlterConfigsRequestFilter) filter).onAlterConfigsRequest(header, (AlterConfigsRequestData) body, filterContext);
            case ALTER_PARTITION_REASSIGNMENTS -> ((AlterPartitionReassignmentsRequestFilter) filter).onAlterPartitionReassignmentsRequest(header,
                    (AlterPartitionReassignmentsRequestData) body, filterContext);
            case ALTER_PARTITION -> ((AlterPartitionRequestFilter) filter).onAlterPartitionRequest(header, (AlterPartitionRequestData) body, filterContext);
            case ALTER_REPLICA_LOG_DIRS ->
                ((AlterReplicaLogDirsRequestFilter) filter).onAlterReplicaLogDirsRequest(header, (AlterReplicaLogDirsRequestData) body, filterContext);
            case ALTER_USER_SCRAM_CREDENTIALS ->
                ((AlterUserScramCredentialsRequestFilter) filter).onAlterUserScramCredentialsRequest(header, (AlterUserScramCredentialsRequestData) body, filterContext);
            case API_VERSIONS -> ((ApiVersionsRequestFilter) filter).onApiVersionsRequest(header, (ApiVersionsRequestData) body, filterContext);
            case BEGIN_QUORUM_EPOCH -> ((BeginQuorumEpochRequestFilter) filter).onBeginQuorumEpochRequest(header, (BeginQuorumEpochRequestData) body, filterContext);
            case BROKER_HEARTBEAT -> ((BrokerHeartbeatRequestFilter) filter).onBrokerHeartbeatRequest(header, (BrokerHeartbeatRequestData) body, filterContext);
            case BROKER_REGISTRATION ->
                ((BrokerRegistrationRequestFilter) filter).onBrokerRegistrationRequest(header, (BrokerRegistrationRequestData) body, filterContext);
            case CONTROLLED_SHUTDOWN ->
                ((ControlledShutdownRequestFilter) filter).onControlledShutdownRequest(header, (ControlledShutdownRequestData) body, filterContext);
            case CREATE_ACLS -> ((CreateAclsRequestFilter) filter).onCreateAclsRequest(header, (CreateAclsRequestData) body, filterContext);
            case CREATE_DELEGATION_TOKEN ->
                ((CreateDelegationTokenRequestFilter) filter).onCreateDelegationTokenRequest(header, (CreateDelegationTokenRequestData) body, filterContext);
            case CREATE_PARTITIONS -> ((CreatePartitionsRequestFilter) filter).onCreatePartitionsRequest(header, (CreatePartitionsRequestData) body, filterContext);
            case CREATE_TOPICS -> ((CreateTopicsRequestFilter) filter).onCreateTopicsRequest(header, (CreateTopicsRequestData) body, filterContext);
            case DELETE_ACLS -> ((DeleteAclsRequestFilter) filter).onDeleteAclsRequest(header, (DeleteAclsRequestData) body, filterContext);
            case DELETE_GROUPS -> ((DeleteGroupsRequestFilter) filter).onDeleteGroupsRequest(header, (DeleteGroupsRequestData) body, filterContext);
            case DELETE_RECORDS -> ((DeleteRecordsRequestFilter) filter).onDeleteRecordsRequest(header, (DeleteRecordsRequestData) body, filterContext);
            case DELETE_TOPICS -> ((DeleteTopicsRequestFilter) filter).onDeleteTopicsRequest(header, (DeleteTopicsRequestData) body, filterContext);
            case DESCRIBE_ACLS -> ((DescribeAclsRequestFilter) filter).onDescribeAclsRequest(header, (DescribeAclsRequestData) body, filterContext);
            case DESCRIBE_CLIENT_QUOTAS ->
                ((DescribeClientQuotasRequestFilter) filter).onDescribeClientQuotasRequest(header, (DescribeClientQuotasRequestData) body, filterContext);
            case DESCRIBE_CLUSTER -> ((DescribeClusterRequestFilter) filter).onDescribeClusterRequest(header, (DescribeClusterRequestData) body, filterContext);
            case DESCRIBE_CONFIGS -> ((DescribeConfigsRequestFilter) filter).onDescribeConfigsRequest(header, (DescribeConfigsRequestData) body, filterContext);
            case DESCRIBE_DELEGATION_TOKEN ->
                ((DescribeDelegationTokenRequestFilter) filter).onDescribeDelegationTokenRequest(header, (DescribeDelegationTokenRequestData) body, filterContext);
            case DESCRIBE_GROUPS -> ((DescribeGroupsRequestFilter) filter).onDescribeGroupsRequest(header, (DescribeGroupsRequestData) body, filterContext);
            case DESCRIBE_LOG_DIRS -> ((DescribeLogDirsRequestFilter) filter).onDescribeLogDirsRequest(header, (DescribeLogDirsRequestData) body, filterContext);
            case DESCRIBE_PRODUCERS -> ((DescribeProducersRequestFilter) filter).onDescribeProducersRequest(header, (DescribeProducersRequestData) body, filterContext);
            case DESCRIBE_QUORUM -> ((DescribeQuorumRequestFilter) filter).onDescribeQuorumRequest(header, (DescribeQuorumRequestData) body, filterContext);
            case DESCRIBE_TRANSACTIONS ->
                ((DescribeTransactionsRequestFilter) filter).onDescribeTransactionsRequest(header, (DescribeTransactionsRequestData) body, filterContext);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> ((DescribeUserScramCredentialsRequestFilter) filter).onDescribeUserScramCredentialsRequest(header,
                    (DescribeUserScramCredentialsRequestData) body, filterContext);
            case ELECT_LEADERS -> ((ElectLeadersRequestFilter) filter).onElectLeadersRequest(header, (ElectLeadersRequestData) body, filterContext);
            case END_QUORUM_EPOCH -> ((EndQuorumEpochRequestFilter) filter).onEndQuorumEpochRequest(header, (EndQuorumEpochRequestData) body, filterContext);
            case END_TXN -> ((EndTxnRequestFilter) filter).onEndTxnRequest(header, (EndTxnRequestData) body, filterContext);
            case ENVELOPE -> ((EnvelopeRequestFilter) filter).onEnvelopeRequest(header, (EnvelopeRequestData) body, filterContext);
            case EXPIRE_DELEGATION_TOKEN ->
                ((ExpireDelegationTokenRequestFilter) filter).onExpireDelegationTokenRequest(header, (ExpireDelegationTokenRequestData) body, filterContext);
            case FETCH -> ((FetchRequestFilter) filter).onFetchRequest(header, (FetchRequestData) body, filterContext);
            case FETCH_SNAPSHOT -> ((FetchSnapshotRequestFilter) filter).onFetchSnapshotRequest(header, (FetchSnapshotRequestData) body, filterContext);
            case FIND_COORDINATOR -> ((FindCoordinatorRequestFilter) filter).onFindCoordinatorRequest(header, (FindCoordinatorRequestData) body, filterContext);
            case HEARTBEAT -> ((HeartbeatRequestFilter) filter).onHeartbeatRequest(header, (HeartbeatRequestData) body, filterContext);
            case INCREMENTAL_ALTER_CONFIGS ->
                ((IncrementalAlterConfigsRequestFilter) filter).onIncrementalAlterConfigsRequest(header, (IncrementalAlterConfigsRequestData) body, filterContext);
            case INIT_PRODUCER_ID -> ((InitProducerIdRequestFilter) filter).onInitProducerIdRequest(header, (InitProducerIdRequestData) body, filterContext);
            case JOIN_GROUP -> ((JoinGroupRequestFilter) filter).onJoinGroupRequest(header, (JoinGroupRequestData) body, filterContext);
            case LEADER_AND_ISR -> ((LeaderAndIsrRequestFilter) filter).onLeaderAndIsrRequest(header, (LeaderAndIsrRequestData) body, filterContext);
            case LEAVE_GROUP -> ((LeaveGroupRequestFilter) filter).onLeaveGroupRequest(header, (LeaveGroupRequestData) body, filterContext);
            case LIST_GROUPS -> ((ListGroupsRequestFilter) filter).onListGroupsRequest(header, (ListGroupsRequestData) body, filterContext);
            case LIST_OFFSETS -> ((ListOffsetsRequestFilter) filter).onListOffsetsRequest(header, (ListOffsetsRequestData) body, filterContext);
            case LIST_PARTITION_REASSIGNMENTS -> ((ListPartitionReassignmentsRequestFilter) filter).onListPartitionReassignmentsRequest(header,
                    (ListPartitionReassignmentsRequestData) body, filterContext);
            case LIST_TRANSACTIONS -> ((ListTransactionsRequestFilter) filter).onListTransactionsRequest(header, (ListTransactionsRequestData) body, filterContext);
            case METADATA -> ((MetadataRequestFilter) filter).onMetadataRequest(header, (MetadataRequestData) body, filterContext);
            case OFFSET_COMMIT -> ((OffsetCommitRequestFilter) filter).onOffsetCommitRequest(header, (OffsetCommitRequestData) body, filterContext);
            case OFFSET_DELETE -> ((OffsetDeleteRequestFilter) filter).onOffsetDeleteRequest(header, (OffsetDeleteRequestData) body, filterContext);
            case OFFSET_FETCH -> ((OffsetFetchRequestFilter) filter).onOffsetFetchRequest(header, (OffsetFetchRequestData) body, filterContext);
            case OFFSET_FOR_LEADER_EPOCH ->
                ((OffsetForLeaderEpochRequestFilter) filter).onOffsetForLeaderEpochRequest(header, (OffsetForLeaderEpochRequestData) body, filterContext);
            case PRODUCE -> ((ProduceRequestFilter) filter).onProduceRequest(header, (ProduceRequestData) body, filterContext);
            case RENEW_DELEGATION_TOKEN ->
                ((RenewDelegationTokenRequestFilter) filter).onRenewDelegationTokenRequest(header, (RenewDelegationTokenRequestData) body, filterContext);
            case SASL_AUTHENTICATE -> ((SaslAuthenticateRequestFilter) filter).onSaslAuthenticateRequest(header, (SaslAuthenticateRequestData) body, filterContext);
            case SASL_HANDSHAKE -> ((SaslHandshakeRequestFilter) filter).onSaslHandshakeRequest(header, (SaslHandshakeRequestData) body, filterContext);
            case STOP_REPLICA -> ((StopReplicaRequestFilter) filter).onStopReplicaRequest(header, (StopReplicaRequestData) body, filterContext);
            case SYNC_GROUP -> ((SyncGroupRequestFilter) filter).onSyncGroupRequest(header, (SyncGroupRequestData) body, filterContext);
            case TXN_OFFSET_COMMIT -> ((TxnOffsetCommitRequestFilter) filter).onTxnOffsetCommitRequest(header, (TxnOffsetCommitRequestData) body, filterContext);
            case UNREGISTER_BROKER -> ((UnregisterBrokerRequestFilter) filter).onUnregisterBrokerRequest(header, (UnregisterBrokerRequestData) body, filterContext);
            case UPDATE_FEATURES -> ((UpdateFeaturesRequestFilter) filter).onUpdateFeaturesRequest(header, (UpdateFeaturesRequestData) body, filterContext);
            case UPDATE_METADATA -> ((UpdateMetadataRequestFilter) filter).onUpdateMetadataRequest(header, (UpdateMetadataRequestData) body, filterContext);
            case VOTE -> ((VoteRequestFilter) filter).onVoteRequest(header, (VoteRequestData) body, filterContext);
            case WRITE_TXN_MARKERS -> ((WriteTxnMarkersRequestFilter) filter).onWriteTxnMarkersRequest(header, (WriteTxnMarkersRequestData) body, filterContext);
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        }
    }

    /**
     * Apply the filter to the given {@code header} and {@code body} using the given {@code filterContext}.
     * @param apiKey The request api key.
     * @param apiVersion The api version.
     * @param header The request header.
     * @param body The request body.
     * @param filterContext The filter context.
     */
    @Override
    public void onResponse(ApiKeys apiKey,
                           short apiVersion,
                           ResponseHeaderData header,
                           ApiMessage body,
                           KrpcFilterContext filterContext) {
        switch (apiKey) {
            case ADD_OFFSETS_TO_TXN -> ((AddOffsetsToTxnResponseFilter) filter).onAddOffsetsToTxnResponse(header, (AddOffsetsToTxnResponseData) body, filterContext);
            case ADD_PARTITIONS_TO_TXN ->
                ((AddPartitionsToTxnResponseFilter) filter).onAddPartitionsToTxnResponse(header, (AddPartitionsToTxnResponseData) body, filterContext);
            case ALLOCATE_PRODUCER_IDS ->
                ((AllocateProducerIdsResponseFilter) filter).onAllocateProducerIdsResponse(header, (AllocateProducerIdsResponseData) body, filterContext);
            case ALTER_CLIENT_QUOTAS ->
                ((AlterClientQuotasResponseFilter) filter).onAlterClientQuotasResponse(header, (AlterClientQuotasResponseData) body, filterContext);
            case ALTER_CONFIGS -> ((AlterConfigsResponseFilter) filter).onAlterConfigsResponse(header, (AlterConfigsResponseData) body, filterContext);
            case ALTER_PARTITION_REASSIGNMENTS -> ((AlterPartitionReassignmentsResponseFilter) filter).onAlterPartitionReassignmentsResponse(header,
                    (AlterPartitionReassignmentsResponseData) body, filterContext);
            case ALTER_PARTITION -> ((AlterPartitionResponseFilter) filter).onAlterPartitionResponse(header, (AlterPartitionResponseData) body, filterContext);
            case ALTER_REPLICA_LOG_DIRS ->
                ((AlterReplicaLogDirsResponseFilter) filter).onAlterReplicaLogDirsResponse(header, (AlterReplicaLogDirsResponseData) body, filterContext);
            case ALTER_USER_SCRAM_CREDENTIALS -> ((AlterUserScramCredentialsResponseFilter) filter).onAlterUserScramCredentialsResponse(header,
                    (AlterUserScramCredentialsResponseData) body, filterContext);
            case API_VERSIONS -> ((ApiVersionsResponseFilter) filter).onApiVersionsResponse(header, (ApiVersionsResponseData) body, filterContext);
            case BEGIN_QUORUM_EPOCH -> ((BeginQuorumEpochResponseFilter) filter).onBeginQuorumEpochResponse(header, (BeginQuorumEpochResponseData) body, filterContext);
            case BROKER_HEARTBEAT -> ((BrokerHeartbeatResponseFilter) filter).onBrokerHeartbeatResponse(header, (BrokerHeartbeatResponseData) body, filterContext);
            case BROKER_REGISTRATION ->
                ((BrokerRegistrationResponseFilter) filter).onBrokerRegistrationResponse(header, (BrokerRegistrationResponseData) body, filterContext);
            case CONTROLLED_SHUTDOWN ->
                ((ControlledShutdownResponseFilter) filter).onControlledShutdownResponse(header, (ControlledShutdownResponseData) body, filterContext);
            case CREATE_ACLS -> ((CreateAclsResponseFilter) filter).onCreateAclsResponse(header, (CreateAclsResponseData) body, filterContext);
            case CREATE_DELEGATION_TOKEN ->
                ((CreateDelegationTokenResponseFilter) filter).onCreateDelegationTokenResponse(header, (CreateDelegationTokenResponseData) body, filterContext);
            case CREATE_PARTITIONS -> ((CreatePartitionsResponseFilter) filter).onCreatePartitionsResponse(header, (CreatePartitionsResponseData) body, filterContext);
            case CREATE_TOPICS -> ((CreateTopicsResponseFilter) filter).onCreateTopicsResponse(header, (CreateTopicsResponseData) body, filterContext);
            case DELETE_ACLS -> ((DeleteAclsResponseFilter) filter).onDeleteAclsResponse(header, (DeleteAclsResponseData) body, filterContext);
            case DELETE_GROUPS -> ((DeleteGroupsResponseFilter) filter).onDeleteGroupsResponse(header, (DeleteGroupsResponseData) body, filterContext);
            case DELETE_RECORDS -> ((DeleteRecordsResponseFilter) filter).onDeleteRecordsResponse(header, (DeleteRecordsResponseData) body, filterContext);
            case DELETE_TOPICS -> ((DeleteTopicsResponseFilter) filter).onDeleteTopicsResponse(header, (DeleteTopicsResponseData) body, filterContext);
            case DESCRIBE_ACLS -> ((DescribeAclsResponseFilter) filter).onDescribeAclsResponse(header, (DescribeAclsResponseData) body, filterContext);
            case DESCRIBE_CLIENT_QUOTAS ->
                ((DescribeClientQuotasResponseFilter) filter).onDescribeClientQuotasResponse(header, (DescribeClientQuotasResponseData) body, filterContext);
            case DESCRIBE_CLUSTER -> ((DescribeClusterResponseFilter) filter).onDescribeClusterResponse(header, (DescribeClusterResponseData) body, filterContext);
            case DESCRIBE_CONFIGS -> ((DescribeConfigsResponseFilter) filter).onDescribeConfigsResponse(header, (DescribeConfigsResponseData) body, filterContext);
            case DESCRIBE_DELEGATION_TOKEN ->
                ((DescribeDelegationTokenResponseFilter) filter).onDescribeDelegationTokenResponse(header, (DescribeDelegationTokenResponseData) body, filterContext);
            case DESCRIBE_GROUPS -> ((DescribeGroupsResponseFilter) filter).onDescribeGroupsResponse(header, (DescribeGroupsResponseData) body, filterContext);
            case DESCRIBE_LOG_DIRS -> ((DescribeLogDirsResponseFilter) filter).onDescribeLogDirsResponse(header, (DescribeLogDirsResponseData) body, filterContext);
            case DESCRIBE_PRODUCERS ->
                ((DescribeProducersResponseFilter) filter).onDescribeProducersResponse(header, (DescribeProducersResponseData) body, filterContext);
            case DESCRIBE_QUORUM -> ((DescribeQuorumResponseFilter) filter).onDescribeQuorumResponse(header, (DescribeQuorumResponseData) body, filterContext);
            case DESCRIBE_TRANSACTIONS ->
                ((DescribeTransactionsResponseFilter) filter).onDescribeTransactionsResponse(header, (DescribeTransactionsResponseData) body, filterContext);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> ((DescribeUserScramCredentialsResponseFilter) filter).onDescribeUserScramCredentialsResponse(header,
                    (DescribeUserScramCredentialsResponseData) body, filterContext);
            case ELECT_LEADERS -> ((ElectLeadersResponseFilter) filter).onElectLeadersResponse(header, (ElectLeadersResponseData) body, filterContext);
            case END_QUORUM_EPOCH -> ((EndQuorumEpochResponseFilter) filter).onEndQuorumEpochResponse(header, (EndQuorumEpochResponseData) body, filterContext);
            case END_TXN -> ((EndTxnResponseFilter) filter).onEndTxnResponse(header, (EndTxnResponseData) body, filterContext);
            case ENVELOPE -> ((EnvelopeResponseFilter) filter).onEnvelopeResponse(header, (EnvelopeResponseData) body, filterContext);
            case EXPIRE_DELEGATION_TOKEN ->
                ((ExpireDelegationTokenResponseFilter) filter).onExpireDelegationTokenResponse(header, (ExpireDelegationTokenResponseData) body, filterContext);
            case FETCH -> ((FetchResponseFilter) filter).onFetchResponse(header, (FetchResponseData) body, filterContext);
            case FETCH_SNAPSHOT -> ((FetchSnapshotResponseFilter) filter).onFetchSnapshotResponse(header, (FetchSnapshotResponseData) body, filterContext);
            case FIND_COORDINATOR -> ((FindCoordinatorResponseFilter) filter).onFindCoordinatorResponse(header, (FindCoordinatorResponseData) body, filterContext);
            case HEARTBEAT -> ((HeartbeatResponseFilter) filter).onHeartbeatResponse(header, (HeartbeatResponseData) body, filterContext);
            case INCREMENTAL_ALTER_CONFIGS ->
                ((IncrementalAlterConfigsResponseFilter) filter).onIncrementalAlterConfigsResponse(header, (IncrementalAlterConfigsResponseData) body, filterContext);
            case INIT_PRODUCER_ID -> ((InitProducerIdResponseFilter) filter).onInitProducerIdResponse(header, (InitProducerIdResponseData) body, filterContext);
            case JOIN_GROUP -> ((JoinGroupResponseFilter) filter).onJoinGroupResponse(header, (JoinGroupResponseData) body, filterContext);
            case LEADER_AND_ISR -> ((LeaderAndIsrResponseFilter) filter).onLeaderAndIsrResponse(header, (LeaderAndIsrResponseData) body, filterContext);
            case LEAVE_GROUP -> ((LeaveGroupResponseFilter) filter).onLeaveGroupResponse(header, (LeaveGroupResponseData) body, filterContext);
            case LIST_GROUPS -> ((ListGroupsResponseFilter) filter).onListGroupsResponse(header, (ListGroupsResponseData) body, filterContext);
            case LIST_OFFSETS -> ((ListOffsetsResponseFilter) filter).onListOffsetsResponse(header, (ListOffsetsResponseData) body, filterContext);
            case LIST_PARTITION_REASSIGNMENTS -> ((ListPartitionReassignmentsResponseFilter) filter).onListPartitionReassignmentsResponse(header,
                    (ListPartitionReassignmentsResponseData) body, filterContext);
            case LIST_TRANSACTIONS -> ((ListTransactionsResponseFilter) filter).onListTransactionsResponse(header, (ListTransactionsResponseData) body, filterContext);
            case METADATA -> ((MetadataResponseFilter) filter).onMetadataResponse(header, (MetadataResponseData) body, filterContext);
            case OFFSET_COMMIT -> ((OffsetCommitResponseFilter) filter).onOffsetCommitResponse(header, (OffsetCommitResponseData) body, filterContext);
            case OFFSET_DELETE -> ((OffsetDeleteResponseFilter) filter).onOffsetDeleteResponse(header, (OffsetDeleteResponseData) body, filterContext);
            case OFFSET_FETCH -> ((OffsetFetchResponseFilter) filter).onOffsetFetchResponse(header, (OffsetFetchResponseData) body, filterContext);
            case OFFSET_FOR_LEADER_EPOCH ->
                ((OffsetForLeaderEpochResponseFilter) filter).onOffsetForLeaderEpochResponse(header, (OffsetForLeaderEpochResponseData) body, filterContext);
            case PRODUCE -> ((ProduceResponseFilter) filter).onProduceResponse(header, (ProduceResponseData) body, filterContext);
            case RENEW_DELEGATION_TOKEN ->
                ((RenewDelegationTokenResponseFilter) filter).onRenewDelegationTokenResponse(header, (RenewDelegationTokenResponseData) body, filterContext);
            case SASL_AUTHENTICATE -> ((SaslAuthenticateResponseFilter) filter).onSaslAuthenticateResponse(header, (SaslAuthenticateResponseData) body, filterContext);
            case SASL_HANDSHAKE -> ((SaslHandshakeResponseFilter) filter).onSaslHandshakeResponse(header, (SaslHandshakeResponseData) body, filterContext);
            case STOP_REPLICA -> ((StopReplicaResponseFilter) filter).onStopReplicaResponse(header, (StopReplicaResponseData) body, filterContext);
            case SYNC_GROUP -> ((SyncGroupResponseFilter) filter).onSyncGroupResponse(header, (SyncGroupResponseData) body, filterContext);
            case TXN_OFFSET_COMMIT -> ((TxnOffsetCommitResponseFilter) filter).onTxnOffsetCommitResponse(header, (TxnOffsetCommitResponseData) body, filterContext);
            case UNREGISTER_BROKER -> ((UnregisterBrokerResponseFilter) filter).onUnregisterBrokerResponse(header, (UnregisterBrokerResponseData) body, filterContext);
            case UPDATE_FEATURES -> ((UpdateFeaturesResponseFilter) filter).onUpdateFeaturesResponse(header, (UpdateFeaturesResponseData) body, filterContext);
            case UPDATE_METADATA -> ((UpdateMetadataResponseFilter) filter).onUpdateMetadataResponse(header, (UpdateMetadataResponseData) body, filterContext);
            case VOTE -> ((VoteResponseFilter) filter).onVoteResponse(header, (VoteResponseData) body, filterContext);
            case WRITE_TXN_MARKERS -> ((WriteTxnMarkersResponseFilter) filter).onWriteTxnMarkersResponse(header, (WriteTxnMarkersResponseData) body, filterContext);
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        }
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
            case ADD_OFFSETS_TO_TXN ->
                filter instanceof AddOffsetsToTxnRequestFilter && ((AddOffsetsToTxnRequestFilter) filter).shouldHandleAddOffsetsToTxnRequest(apiVersion);
            case ADD_PARTITIONS_TO_TXN ->
                filter instanceof AddPartitionsToTxnRequestFilter && ((AddPartitionsToTxnRequestFilter) filter).shouldHandleAddPartitionsToTxnRequest(apiVersion);
            case ALLOCATE_PRODUCER_IDS ->
                filter instanceof AllocateProducerIdsRequestFilter && ((AllocateProducerIdsRequestFilter) filter).shouldHandleAllocateProducerIdsRequest(apiVersion);
            case ALTER_CLIENT_QUOTAS ->
                filter instanceof AlterClientQuotasRequestFilter && ((AlterClientQuotasRequestFilter) filter).shouldHandleAlterClientQuotasRequest(apiVersion);
            case ALTER_CONFIGS -> filter instanceof AlterConfigsRequestFilter && ((AlterConfigsRequestFilter) filter).shouldHandleAlterConfigsRequest(apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> filter instanceof AlterPartitionReassignmentsRequestFilter
                    && ((AlterPartitionReassignmentsRequestFilter) filter).shouldHandleAlterPartitionReassignmentsRequest(apiVersion);
            case ALTER_PARTITION -> filter instanceof AlterPartitionRequestFilter && ((AlterPartitionRequestFilter) filter).shouldHandleAlterPartitionRequest(apiVersion);
            case ALTER_REPLICA_LOG_DIRS ->
                filter instanceof AlterReplicaLogDirsRequestFilter && ((AlterReplicaLogDirsRequestFilter) filter).shouldHandleAlterReplicaLogDirsRequest(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> filter instanceof AlterUserScramCredentialsRequestFilter
                    && ((AlterUserScramCredentialsRequestFilter) filter).shouldHandleAlterUserScramCredentialsRequest(apiVersion);
            case API_VERSIONS -> filter instanceof ApiVersionsRequestFilter && ((ApiVersionsRequestFilter) filter).shouldHandleApiVersionsRequest(apiVersion);
            case BEGIN_QUORUM_EPOCH ->
                filter instanceof BeginQuorumEpochRequestFilter && ((BeginQuorumEpochRequestFilter) filter).shouldHandleBeginQuorumEpochRequest(apiVersion);
            case BROKER_HEARTBEAT ->
                filter instanceof BrokerHeartbeatRequestFilter && ((BrokerHeartbeatRequestFilter) filter).shouldHandleBrokerHeartbeatRequest(apiVersion);
            case BROKER_REGISTRATION ->
                filter instanceof BrokerRegistrationRequestFilter && ((BrokerRegistrationRequestFilter) filter).shouldHandleBrokerRegistrationRequest(apiVersion);
            case CONTROLLED_SHUTDOWN ->
                filter instanceof ControlledShutdownRequestFilter && ((ControlledShutdownRequestFilter) filter).shouldHandleControlledShutdownRequest(apiVersion);
            case CREATE_ACLS -> filter instanceof CreateAclsRequestFilter && ((CreateAclsRequestFilter) filter).shouldHandleCreateAclsRequest(apiVersion);
            case CREATE_DELEGATION_TOKEN -> filter instanceof CreateDelegationTokenRequestFilter
                    && ((CreateDelegationTokenRequestFilter) filter).shouldHandleCreateDelegationTokenRequest(apiVersion);
            case CREATE_PARTITIONS ->
                filter instanceof CreatePartitionsRequestFilter && ((CreatePartitionsRequestFilter) filter).shouldHandleCreatePartitionsRequest(apiVersion);
            case CREATE_TOPICS -> filter instanceof CreateTopicsRequestFilter && ((CreateTopicsRequestFilter) filter).shouldHandleCreateTopicsRequest(apiVersion);
            case DELETE_ACLS -> filter instanceof DeleteAclsRequestFilter && ((DeleteAclsRequestFilter) filter).shouldHandleDeleteAclsRequest(apiVersion);
            case DELETE_GROUPS -> filter instanceof DeleteGroupsRequestFilter && ((DeleteGroupsRequestFilter) filter).shouldHandleDeleteGroupsRequest(apiVersion);
            case DELETE_RECORDS -> filter instanceof DeleteRecordsRequestFilter && ((DeleteRecordsRequestFilter) filter).shouldHandleDeleteRecordsRequest(apiVersion);
            case DELETE_TOPICS -> filter instanceof DeleteTopicsRequestFilter && ((DeleteTopicsRequestFilter) filter).shouldHandleDeleteTopicsRequest(apiVersion);
            case DESCRIBE_ACLS -> filter instanceof DescribeAclsRequestFilter && ((DescribeAclsRequestFilter) filter).shouldHandleDescribeAclsRequest(apiVersion);
            case DESCRIBE_CLIENT_QUOTAS ->
                filter instanceof DescribeClientQuotasRequestFilter && ((DescribeClientQuotasRequestFilter) filter).shouldHandleDescribeClientQuotasRequest(apiVersion);
            case DESCRIBE_CLUSTER ->
                filter instanceof DescribeClusterRequestFilter && ((DescribeClusterRequestFilter) filter).shouldHandleDescribeClusterRequest(apiVersion);
            case DESCRIBE_CONFIGS ->
                filter instanceof DescribeConfigsRequestFilter && ((DescribeConfigsRequestFilter) filter).shouldHandleDescribeConfigsRequest(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> filter instanceof DescribeDelegationTokenRequestFilter
                    && ((DescribeDelegationTokenRequestFilter) filter).shouldHandleDescribeDelegationTokenRequest(apiVersion);
            case DESCRIBE_GROUPS -> filter instanceof DescribeGroupsRequestFilter && ((DescribeGroupsRequestFilter) filter).shouldHandleDescribeGroupsRequest(apiVersion);
            case DESCRIBE_LOG_DIRS ->
                filter instanceof DescribeLogDirsRequestFilter && ((DescribeLogDirsRequestFilter) filter).shouldHandleDescribeLogDirsRequest(apiVersion);
            case DESCRIBE_PRODUCERS ->
                filter instanceof DescribeProducersRequestFilter && ((DescribeProducersRequestFilter) filter).shouldHandleDescribeProducersRequest(apiVersion);
            case DESCRIBE_QUORUM -> filter instanceof DescribeQuorumRequestFilter && ((DescribeQuorumRequestFilter) filter).shouldHandleDescribeQuorumRequest(apiVersion);
            case DESCRIBE_TRANSACTIONS ->
                filter instanceof DescribeTransactionsRequestFilter && ((DescribeTransactionsRequestFilter) filter).shouldHandleDescribeTransactionsRequest(apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> filter instanceof DescribeUserScramCredentialsRequestFilter
                    && ((DescribeUserScramCredentialsRequestFilter) filter).shouldHandleDescribeUserScramCredentialsRequest(apiVersion);
            case ELECT_LEADERS -> filter instanceof ElectLeadersRequestFilter && ((ElectLeadersRequestFilter) filter).shouldHandleElectLeadersRequest(apiVersion);
            case END_QUORUM_EPOCH ->
                filter instanceof EndQuorumEpochRequestFilter && ((EndQuorumEpochRequestFilter) filter).shouldHandleEndQuorumEpochRequest(apiVersion);
            case END_TXN -> filter instanceof EndTxnRequestFilter && ((EndTxnRequestFilter) filter).shouldHandleEndTxnRequest(apiVersion);
            case ENVELOPE -> filter instanceof EnvelopeRequestFilter && ((EnvelopeRequestFilter) filter).shouldHandleEnvelopeRequest(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> filter instanceof ExpireDelegationTokenRequestFilter
                    && ((ExpireDelegationTokenRequestFilter) filter).shouldHandleExpireDelegationTokenRequest(apiVersion);
            case FETCH -> filter instanceof FetchRequestFilter && ((FetchRequestFilter) filter).shouldHandleFetchRequest(apiVersion);
            case FETCH_SNAPSHOT -> filter instanceof FetchSnapshotRequestFilter && ((FetchSnapshotRequestFilter) filter).shouldHandleFetchSnapshotRequest(apiVersion);
            case FIND_COORDINATOR ->
                filter instanceof FindCoordinatorRequestFilter && ((FindCoordinatorRequestFilter) filter).shouldHandleFindCoordinatorRequest(apiVersion);
            case HEARTBEAT -> filter instanceof HeartbeatRequestFilter && ((HeartbeatRequestFilter) filter).shouldHandleHeartbeatRequest(apiVersion);
            case INCREMENTAL_ALTER_CONFIGS -> filter instanceof IncrementalAlterConfigsRequestFilter
                    && ((IncrementalAlterConfigsRequestFilter) filter).shouldHandleIncrementalAlterConfigsRequest(apiVersion);
            case INIT_PRODUCER_ID ->
                filter instanceof InitProducerIdRequestFilter && ((InitProducerIdRequestFilter) filter).shouldHandleInitProducerIdRequest(apiVersion);
            case JOIN_GROUP -> filter instanceof JoinGroupRequestFilter && ((JoinGroupRequestFilter) filter).shouldHandleJoinGroupRequest(apiVersion);
            case LEADER_AND_ISR -> filter instanceof LeaderAndIsrRequestFilter && ((LeaderAndIsrRequestFilter) filter).shouldHandleLeaderAndIsrRequest(apiVersion);
            case LEAVE_GROUP -> filter instanceof LeaveGroupRequestFilter && ((LeaveGroupRequestFilter) filter).shouldHandleLeaveGroupRequest(apiVersion);
            case LIST_GROUPS -> filter instanceof ListGroupsRequestFilter && ((ListGroupsRequestFilter) filter).shouldHandleListGroupsRequest(apiVersion);
            case LIST_OFFSETS -> filter instanceof ListOffsetsRequestFilter && ((ListOffsetsRequestFilter) filter).shouldHandleListOffsetsRequest(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> filter instanceof ListPartitionReassignmentsRequestFilter
                    && ((ListPartitionReassignmentsRequestFilter) filter).shouldHandleListPartitionReassignmentsRequest(apiVersion);
            case LIST_TRANSACTIONS ->
                filter instanceof ListTransactionsRequestFilter && ((ListTransactionsRequestFilter) filter).shouldHandleListTransactionsRequest(apiVersion);
            case METADATA -> filter instanceof MetadataRequestFilter && ((MetadataRequestFilter) filter).shouldHandleMetadataRequest(apiVersion);
            case OFFSET_COMMIT -> filter instanceof OffsetCommitRequestFilter && ((OffsetCommitRequestFilter) filter).shouldHandleOffsetCommitRequest(apiVersion);
            case OFFSET_DELETE -> filter instanceof OffsetDeleteRequestFilter && ((OffsetDeleteRequestFilter) filter).shouldHandleOffsetDeleteRequest(apiVersion);
            case OFFSET_FETCH -> filter instanceof OffsetFetchRequestFilter && ((OffsetFetchRequestFilter) filter).shouldHandleOffsetFetchRequest(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH ->
                filter instanceof OffsetForLeaderEpochRequestFilter && ((OffsetForLeaderEpochRequestFilter) filter).shouldHandleOffsetForLeaderEpochRequest(apiVersion);
            case PRODUCE -> filter instanceof ProduceRequestFilter && ((ProduceRequestFilter) filter).shouldHandleProduceRequest(apiVersion);
            case RENEW_DELEGATION_TOKEN ->
                filter instanceof RenewDelegationTokenRequestFilter && ((RenewDelegationTokenRequestFilter) filter).shouldHandleRenewDelegationTokenRequest(apiVersion);
            case SASL_AUTHENTICATE ->
                filter instanceof SaslAuthenticateRequestFilter && ((SaslAuthenticateRequestFilter) filter).shouldHandleSaslAuthenticateRequest(apiVersion);
            case SASL_HANDSHAKE -> filter instanceof SaslHandshakeRequestFilter && ((SaslHandshakeRequestFilter) filter).shouldHandleSaslHandshakeRequest(apiVersion);
            case STOP_REPLICA -> filter instanceof StopReplicaRequestFilter && ((StopReplicaRequestFilter) filter).shouldHandleStopReplicaRequest(apiVersion);
            case SYNC_GROUP -> filter instanceof SyncGroupRequestFilter && ((SyncGroupRequestFilter) filter).shouldHandleSyncGroupRequest(apiVersion);
            case TXN_OFFSET_COMMIT ->
                filter instanceof TxnOffsetCommitRequestFilter && ((TxnOffsetCommitRequestFilter) filter).shouldHandleTxnOffsetCommitRequest(apiVersion);
            case UNREGISTER_BROKER ->
                filter instanceof UnregisterBrokerRequestFilter && ((UnregisterBrokerRequestFilter) filter).shouldHandleUnregisterBrokerRequest(apiVersion);
            case UPDATE_FEATURES -> filter instanceof UpdateFeaturesRequestFilter && ((UpdateFeaturesRequestFilter) filter).shouldHandleUpdateFeaturesRequest(apiVersion);
            case UPDATE_METADATA -> filter instanceof UpdateMetadataRequestFilter && ((UpdateMetadataRequestFilter) filter).shouldHandleUpdateMetadataRequest(apiVersion);
            case VOTE -> filter instanceof VoteRequestFilter && ((VoteRequestFilter) filter).shouldHandleVoteRequest(apiVersion);
            case WRITE_TXN_MARKERS ->
                filter instanceof WriteTxnMarkersRequestFilter && ((WriteTxnMarkersRequestFilter) filter).shouldHandleWriteTxnMarkersRequest(apiVersion);
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
            case ADD_OFFSETS_TO_TXN ->
                filter instanceof AddOffsetsToTxnResponseFilter && ((AddOffsetsToTxnResponseFilter) filter).shouldHandleAddOffsetsToTxnResponse(apiVersion);
            case ADD_PARTITIONS_TO_TXN ->
                filter instanceof AddPartitionsToTxnResponseFilter && ((AddPartitionsToTxnResponseFilter) filter).shouldHandleAddPartitionsToTxnResponse(apiVersion);
            case ALLOCATE_PRODUCER_IDS ->
                filter instanceof AllocateProducerIdsResponseFilter && ((AllocateProducerIdsResponseFilter) filter).shouldHandleAllocateProducerIdsResponse(apiVersion);
            case ALTER_CLIENT_QUOTAS ->
                filter instanceof AlterClientQuotasResponseFilter && ((AlterClientQuotasResponseFilter) filter).shouldHandleAlterClientQuotasResponse(apiVersion);
            case ALTER_CONFIGS -> filter instanceof AlterConfigsResponseFilter && ((AlterConfigsResponseFilter) filter).shouldHandleAlterConfigsResponse(apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> filter instanceof AlterPartitionReassignmentsResponseFilter
                    && ((AlterPartitionReassignmentsResponseFilter) filter).shouldHandleAlterPartitionReassignmentsResponse(apiVersion);
            case ALTER_PARTITION ->
                filter instanceof AlterPartitionResponseFilter && ((AlterPartitionResponseFilter) filter).shouldHandleAlterPartitionResponse(apiVersion);
            case ALTER_REPLICA_LOG_DIRS ->
                filter instanceof AlterReplicaLogDirsResponseFilter && ((AlterReplicaLogDirsResponseFilter) filter).shouldHandleAlterReplicaLogDirsResponse(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> filter instanceof AlterUserScramCredentialsResponseFilter
                    && ((AlterUserScramCredentialsResponseFilter) filter).shouldHandleAlterUserScramCredentialsResponse(apiVersion);
            case API_VERSIONS -> filter instanceof ApiVersionsResponseFilter && ((ApiVersionsResponseFilter) filter).shouldHandleApiVersionsResponse(apiVersion);
            case BEGIN_QUORUM_EPOCH ->
                filter instanceof BeginQuorumEpochResponseFilter && ((BeginQuorumEpochResponseFilter) filter).shouldHandleBeginQuorumEpochResponse(apiVersion);
            case BROKER_HEARTBEAT ->
                filter instanceof BrokerHeartbeatResponseFilter && ((BrokerHeartbeatResponseFilter) filter).shouldHandleBrokerHeartbeatResponse(apiVersion);
            case BROKER_REGISTRATION ->
                filter instanceof BrokerRegistrationResponseFilter && ((BrokerRegistrationResponseFilter) filter).shouldHandleBrokerRegistrationResponse(apiVersion);
            case CONTROLLED_SHUTDOWN ->
                filter instanceof ControlledShutdownResponseFilter && ((ControlledShutdownResponseFilter) filter).shouldHandleControlledShutdownResponse(apiVersion);
            case CREATE_ACLS -> filter instanceof CreateAclsResponseFilter && ((CreateAclsResponseFilter) filter).shouldHandleCreateAclsResponse(apiVersion);
            case CREATE_DELEGATION_TOKEN -> filter instanceof CreateDelegationTokenResponseFilter
                    && ((CreateDelegationTokenResponseFilter) filter).shouldHandleCreateDelegationTokenResponse(apiVersion);
            case CREATE_PARTITIONS ->
                filter instanceof CreatePartitionsResponseFilter && ((CreatePartitionsResponseFilter) filter).shouldHandleCreatePartitionsResponse(apiVersion);
            case CREATE_TOPICS -> filter instanceof CreateTopicsResponseFilter && ((CreateTopicsResponseFilter) filter).shouldHandleCreateTopicsResponse(apiVersion);
            case DELETE_ACLS -> filter instanceof DeleteAclsResponseFilter && ((DeleteAclsResponseFilter) filter).shouldHandleDeleteAclsResponse(apiVersion);
            case DELETE_GROUPS -> filter instanceof DeleteGroupsResponseFilter && ((DeleteGroupsResponseFilter) filter).shouldHandleDeleteGroupsResponse(apiVersion);
            case DELETE_RECORDS -> filter instanceof DeleteRecordsResponseFilter && ((DeleteRecordsResponseFilter) filter).shouldHandleDeleteRecordsResponse(apiVersion);
            case DELETE_TOPICS -> filter instanceof DeleteTopicsResponseFilter && ((DeleteTopicsResponseFilter) filter).shouldHandleDeleteTopicsResponse(apiVersion);
            case DESCRIBE_ACLS -> filter instanceof DescribeAclsResponseFilter && ((DescribeAclsResponseFilter) filter).shouldHandleDescribeAclsResponse(apiVersion);
            case DESCRIBE_CLIENT_QUOTAS -> filter instanceof DescribeClientQuotasResponseFilter
                    && ((DescribeClientQuotasResponseFilter) filter).shouldHandleDescribeClientQuotasResponse(apiVersion);
            case DESCRIBE_CLUSTER ->
                filter instanceof DescribeClusterResponseFilter && ((DescribeClusterResponseFilter) filter).shouldHandleDescribeClusterResponse(apiVersion);
            case DESCRIBE_CONFIGS ->
                filter instanceof DescribeConfigsResponseFilter && ((DescribeConfigsResponseFilter) filter).shouldHandleDescribeConfigsResponse(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> filter instanceof DescribeDelegationTokenResponseFilter
                    && ((DescribeDelegationTokenResponseFilter) filter).shouldHandleDescribeDelegationTokenResponse(apiVersion);
            case DESCRIBE_GROUPS ->
                filter instanceof DescribeGroupsResponseFilter && ((DescribeGroupsResponseFilter) filter).shouldHandleDescribeGroupsResponse(apiVersion);
            case DESCRIBE_LOG_DIRS ->
                filter instanceof DescribeLogDirsResponseFilter && ((DescribeLogDirsResponseFilter) filter).shouldHandleDescribeLogDirsResponse(apiVersion);
            case DESCRIBE_PRODUCERS ->
                filter instanceof DescribeProducersResponseFilter && ((DescribeProducersResponseFilter) filter).shouldHandleDescribeProducersResponse(apiVersion);
            case DESCRIBE_QUORUM ->
                filter instanceof DescribeQuorumResponseFilter && ((DescribeQuorumResponseFilter) filter).shouldHandleDescribeQuorumResponse(apiVersion);
            case DESCRIBE_TRANSACTIONS -> filter instanceof DescribeTransactionsResponseFilter
                    && ((DescribeTransactionsResponseFilter) filter).shouldHandleDescribeTransactionsResponse(apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> filter instanceof DescribeUserScramCredentialsResponseFilter
                    && ((DescribeUserScramCredentialsResponseFilter) filter).shouldHandleDescribeUserScramCredentialsResponse(apiVersion);
            case ELECT_LEADERS -> filter instanceof ElectLeadersResponseFilter && ((ElectLeadersResponseFilter) filter).shouldHandleElectLeadersResponse(apiVersion);
            case END_QUORUM_EPOCH ->
                filter instanceof EndQuorumEpochResponseFilter && ((EndQuorumEpochResponseFilter) filter).shouldHandleEndQuorumEpochResponse(apiVersion);
            case END_TXN -> filter instanceof EndTxnResponseFilter && ((EndTxnResponseFilter) filter).shouldHandleEndTxnResponse(apiVersion);
            case ENVELOPE -> filter instanceof EnvelopeResponseFilter && ((EnvelopeResponseFilter) filter).shouldHandleEnvelopeResponse(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> filter instanceof ExpireDelegationTokenResponseFilter
                    && ((ExpireDelegationTokenResponseFilter) filter).shouldHandleExpireDelegationTokenResponse(apiVersion);
            case FETCH -> filter instanceof FetchResponseFilter && ((FetchResponseFilter) filter).shouldHandleFetchResponse(apiVersion);
            case FETCH_SNAPSHOT -> filter instanceof FetchSnapshotResponseFilter && ((FetchSnapshotResponseFilter) filter).shouldHandleFetchSnapshotResponse(apiVersion);
            case FIND_COORDINATOR ->
                filter instanceof FindCoordinatorResponseFilter && ((FindCoordinatorResponseFilter) filter).shouldHandleFindCoordinatorResponse(apiVersion);
            case HEARTBEAT -> filter instanceof HeartbeatResponseFilter && ((HeartbeatResponseFilter) filter).shouldHandleHeartbeatResponse(apiVersion);
            case INCREMENTAL_ALTER_CONFIGS -> filter instanceof IncrementalAlterConfigsResponseFilter
                    && ((IncrementalAlterConfigsResponseFilter) filter).shouldHandleIncrementalAlterConfigsResponse(apiVersion);
            case INIT_PRODUCER_ID ->
                filter instanceof InitProducerIdResponseFilter && ((InitProducerIdResponseFilter) filter).shouldHandleInitProducerIdResponse(apiVersion);
            case JOIN_GROUP -> filter instanceof JoinGroupResponseFilter && ((JoinGroupResponseFilter) filter).shouldHandleJoinGroupResponse(apiVersion);
            case LEADER_AND_ISR -> filter instanceof LeaderAndIsrResponseFilter && ((LeaderAndIsrResponseFilter) filter).shouldHandleLeaderAndIsrResponse(apiVersion);
            case LEAVE_GROUP -> filter instanceof LeaveGroupResponseFilter && ((LeaveGroupResponseFilter) filter).shouldHandleLeaveGroupResponse(apiVersion);
            case LIST_GROUPS -> filter instanceof ListGroupsResponseFilter && ((ListGroupsResponseFilter) filter).shouldHandleListGroupsResponse(apiVersion);
            case LIST_OFFSETS -> filter instanceof ListOffsetsResponseFilter && ((ListOffsetsResponseFilter) filter).shouldHandleListOffsetsResponse(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> filter instanceof ListPartitionReassignmentsResponseFilter
                    && ((ListPartitionReassignmentsResponseFilter) filter).shouldHandleListPartitionReassignmentsResponse(apiVersion);
            case LIST_TRANSACTIONS ->
                filter instanceof ListTransactionsResponseFilter && ((ListTransactionsResponseFilter) filter).shouldHandleListTransactionsResponse(apiVersion);
            case METADATA -> filter instanceof MetadataResponseFilter && ((MetadataResponseFilter) filter).shouldHandleMetadataResponse(apiVersion);
            case OFFSET_COMMIT -> filter instanceof OffsetCommitResponseFilter && ((OffsetCommitResponseFilter) filter).shouldHandleOffsetCommitResponse(apiVersion);
            case OFFSET_DELETE -> filter instanceof OffsetDeleteResponseFilter && ((OffsetDeleteResponseFilter) filter).shouldHandleOffsetDeleteResponse(apiVersion);
            case OFFSET_FETCH -> filter instanceof OffsetFetchResponseFilter && ((OffsetFetchResponseFilter) filter).shouldHandleOffsetFetchResponse(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH -> filter instanceof OffsetForLeaderEpochResponseFilter
                    && ((OffsetForLeaderEpochResponseFilter) filter).shouldHandleOffsetForLeaderEpochResponse(apiVersion);
            case PRODUCE -> filter instanceof ProduceResponseFilter && ((ProduceResponseFilter) filter).shouldHandleProduceResponse(apiVersion);
            case RENEW_DELEGATION_TOKEN -> filter instanceof RenewDelegationTokenResponseFilter
                    && ((RenewDelegationTokenResponseFilter) filter).shouldHandleRenewDelegationTokenResponse(apiVersion);
            case SASL_AUTHENTICATE ->
                filter instanceof SaslAuthenticateResponseFilter && ((SaslAuthenticateResponseFilter) filter).shouldHandleSaslAuthenticateResponse(apiVersion);
            case SASL_HANDSHAKE -> filter instanceof SaslHandshakeResponseFilter && ((SaslHandshakeResponseFilter) filter).shouldHandleSaslHandshakeResponse(apiVersion);
            case STOP_REPLICA -> filter instanceof StopReplicaResponseFilter && ((StopReplicaResponseFilter) filter).shouldHandleStopReplicaResponse(apiVersion);
            case SYNC_GROUP -> filter instanceof SyncGroupResponseFilter && ((SyncGroupResponseFilter) filter).shouldHandleSyncGroupResponse(apiVersion);
            case TXN_OFFSET_COMMIT ->
                filter instanceof TxnOffsetCommitResponseFilter && ((TxnOffsetCommitResponseFilter) filter).shouldHandleTxnOffsetCommitResponse(apiVersion);
            case UNREGISTER_BROKER ->
                filter instanceof UnregisterBrokerResponseFilter && ((UnregisterBrokerResponseFilter) filter).shouldHandleUnregisterBrokerResponse(apiVersion);
            case UPDATE_FEATURES ->
                filter instanceof UpdateFeaturesResponseFilter && ((UpdateFeaturesResponseFilter) filter).shouldHandleUpdateFeaturesResponse(apiVersion);
            case UPDATE_METADATA ->
                filter instanceof UpdateMetadataResponseFilter && ((UpdateMetadataResponseFilter) filter).shouldHandleUpdateMetadataResponse(apiVersion);
            case VOTE -> filter instanceof VoteResponseFilter && ((VoteResponseFilter) filter).shouldHandleVoteResponse(apiVersion);
            case WRITE_TXN_MARKERS ->
                filter instanceof WriteTxnMarkersResponseFilter && ((WriteTxnMarkersResponseFilter) filter).shouldHandleWriteTxnMarkersResponse(apiVersion);
            default -> throw new IllegalStateException("Unsupported API key " + apiKey);
        };
    }

    /**
     * Check if a KrpcFilter implements any of the Specific Message Filter interfaces
     * @param filter the filter
     * @return true if the filter implements any Specific Message Filter interfaces
     */
    public static boolean implementsAnySpecificFilterInterface(KrpcFilter filter) {
        return filter instanceof AddOffsetsToTxnRequestFilter ||
                filter instanceof AddOffsetsToTxnResponseFilter ||
                filter instanceof AddPartitionsToTxnRequestFilter ||
                filter instanceof AddPartitionsToTxnResponseFilter ||
                filter instanceof AllocateProducerIdsRequestFilter ||
                filter instanceof AllocateProducerIdsResponseFilter ||
                filter instanceof AlterClientQuotasRequestFilter ||
                filter instanceof AlterClientQuotasResponseFilter ||
                filter instanceof AlterConfigsRequestFilter ||
                filter instanceof AlterConfigsResponseFilter ||
                filter instanceof AlterPartitionReassignmentsRequestFilter ||
                filter instanceof AlterPartitionReassignmentsResponseFilter ||
                filter instanceof AlterPartitionRequestFilter ||
                filter instanceof AlterPartitionResponseFilter ||
                filter instanceof AlterReplicaLogDirsRequestFilter ||
                filter instanceof AlterReplicaLogDirsResponseFilter ||
                filter instanceof AlterUserScramCredentialsRequestFilter ||
                filter instanceof AlterUserScramCredentialsResponseFilter ||
                filter instanceof ApiVersionsRequestFilter ||
                filter instanceof ApiVersionsResponseFilter ||
                filter instanceof BeginQuorumEpochRequestFilter ||
                filter instanceof BeginQuorumEpochResponseFilter ||
                filter instanceof BrokerHeartbeatRequestFilter ||
                filter instanceof BrokerHeartbeatResponseFilter ||
                filter instanceof BrokerRegistrationRequestFilter ||
                filter instanceof BrokerRegistrationResponseFilter ||
                filter instanceof ControlledShutdownRequestFilter ||
                filter instanceof ControlledShutdownResponseFilter ||
                filter instanceof CreateAclsRequestFilter ||
                filter instanceof CreateAclsResponseFilter ||
                filter instanceof CreateDelegationTokenRequestFilter ||
                filter instanceof CreateDelegationTokenResponseFilter ||
                filter instanceof CreatePartitionsRequestFilter ||
                filter instanceof CreatePartitionsResponseFilter ||
                filter instanceof CreateTopicsRequestFilter ||
                filter instanceof CreateTopicsResponseFilter ||
                filter instanceof DeleteAclsRequestFilter ||
                filter instanceof DeleteAclsResponseFilter ||
                filter instanceof DeleteGroupsRequestFilter ||
                filter instanceof DeleteGroupsResponseFilter ||
                filter instanceof DeleteRecordsRequestFilter ||
                filter instanceof DeleteRecordsResponseFilter ||
                filter instanceof DeleteTopicsRequestFilter ||
                filter instanceof DeleteTopicsResponseFilter ||
                filter instanceof DescribeAclsRequestFilter ||
                filter instanceof DescribeAclsResponseFilter ||
                filter instanceof DescribeClientQuotasRequestFilter ||
                filter instanceof DescribeClientQuotasResponseFilter ||
                filter instanceof DescribeClusterRequestFilter ||
                filter instanceof DescribeClusterResponseFilter ||
                filter instanceof DescribeConfigsRequestFilter ||
                filter instanceof DescribeConfigsResponseFilter ||
                filter instanceof DescribeDelegationTokenRequestFilter ||
                filter instanceof DescribeDelegationTokenResponseFilter ||
                filter instanceof DescribeGroupsRequestFilter ||
                filter instanceof DescribeGroupsResponseFilter ||
                filter instanceof DescribeLogDirsRequestFilter ||
                filter instanceof DescribeLogDirsResponseFilter ||
                filter instanceof DescribeProducersRequestFilter ||
                filter instanceof DescribeProducersResponseFilter ||
                filter instanceof DescribeQuorumRequestFilter ||
                filter instanceof DescribeQuorumResponseFilter ||
                filter instanceof DescribeTransactionsRequestFilter ||
                filter instanceof DescribeTransactionsResponseFilter ||
                filter instanceof DescribeUserScramCredentialsRequestFilter ||
                filter instanceof DescribeUserScramCredentialsResponseFilter ||
                filter instanceof ElectLeadersRequestFilter ||
                filter instanceof ElectLeadersResponseFilter ||
                filter instanceof EndQuorumEpochRequestFilter ||
                filter instanceof EndQuorumEpochResponseFilter ||
                filter instanceof EndTxnRequestFilter ||
                filter instanceof EndTxnResponseFilter ||
                filter instanceof EnvelopeRequestFilter ||
                filter instanceof EnvelopeResponseFilter ||
                filter instanceof ExpireDelegationTokenRequestFilter ||
                filter instanceof ExpireDelegationTokenResponseFilter ||
                filter instanceof FetchRequestFilter ||
                filter instanceof FetchResponseFilter ||
                filter instanceof FetchSnapshotRequestFilter ||
                filter instanceof FetchSnapshotResponseFilter ||
                filter instanceof FindCoordinatorRequestFilter ||
                filter instanceof FindCoordinatorResponseFilter ||
                filter instanceof HeartbeatRequestFilter ||
                filter instanceof HeartbeatResponseFilter ||
                filter instanceof IncrementalAlterConfigsRequestFilter ||
                filter instanceof IncrementalAlterConfigsResponseFilter ||
                filter instanceof InitProducerIdRequestFilter ||
                filter instanceof InitProducerIdResponseFilter ||
                filter instanceof JoinGroupRequestFilter ||
                filter instanceof JoinGroupResponseFilter ||
                filter instanceof LeaderAndIsrRequestFilter ||
                filter instanceof LeaderAndIsrResponseFilter ||
                filter instanceof LeaveGroupRequestFilter ||
                filter instanceof LeaveGroupResponseFilter ||
                filter instanceof ListGroupsRequestFilter ||
                filter instanceof ListGroupsResponseFilter ||
                filter instanceof ListOffsetsRequestFilter ||
                filter instanceof ListOffsetsResponseFilter ||
                filter instanceof ListPartitionReassignmentsRequestFilter ||
                filter instanceof ListPartitionReassignmentsResponseFilter ||
                filter instanceof ListTransactionsRequestFilter ||
                filter instanceof ListTransactionsResponseFilter ||
                filter instanceof MetadataRequestFilter ||
                filter instanceof MetadataResponseFilter ||
                filter instanceof OffsetCommitRequestFilter ||
                filter instanceof OffsetCommitResponseFilter ||
                filter instanceof OffsetDeleteRequestFilter ||
                filter instanceof OffsetDeleteResponseFilter ||
                filter instanceof OffsetFetchRequestFilter ||
                filter instanceof OffsetFetchResponseFilter ||
                filter instanceof OffsetForLeaderEpochRequestFilter ||
                filter instanceof OffsetForLeaderEpochResponseFilter ||
                filter instanceof ProduceRequestFilter ||
                filter instanceof ProduceResponseFilter ||
                filter instanceof RenewDelegationTokenRequestFilter ||
                filter instanceof RenewDelegationTokenResponseFilter ||
                filter instanceof SaslAuthenticateRequestFilter ||
                filter instanceof SaslAuthenticateResponseFilter ||
                filter instanceof SaslHandshakeRequestFilter ||
                filter instanceof SaslHandshakeResponseFilter ||
                filter instanceof StopReplicaRequestFilter ||
                filter instanceof StopReplicaResponseFilter ||
                filter instanceof SyncGroupRequestFilter ||
                filter instanceof SyncGroupResponseFilter ||
                filter instanceof TxnOffsetCommitRequestFilter ||
                filter instanceof TxnOffsetCommitResponseFilter ||
                filter instanceof UnregisterBrokerRequestFilter ||
                filter instanceof UnregisterBrokerResponseFilter ||
                filter instanceof UpdateFeaturesRequestFilter ||
                filter instanceof UpdateFeaturesResponseFilter ||
                filter instanceof UpdateMetadataRequestFilter ||
                filter instanceof UpdateMetadataResponseFilter ||
                filter instanceof VoteRequestFilter ||
                filter instanceof VoteResponseFilter ||
                filter instanceof WriteTxnMarkersRequestFilter ||
                filter instanceof WriteTxnMarkersResponseFilter;
    }

}
