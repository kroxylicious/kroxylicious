/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarking.jmh;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.AddOffsetsToTxnRequestData;
import io.kroxylicious.kafka.common.message.AddOffsetsToTxnResponseData;
import io.kroxylicious.kafka.common.message.AddPartitionsToTxnRequestData;
import io.kroxylicious.kafka.common.message.AddPartitionsToTxnResponseData;
import io.kroxylicious.kafka.common.message.AllocateProducerIdsRequestData;
import io.kroxylicious.kafka.common.message.AllocateProducerIdsResponseData;
import io.kroxylicious.kafka.common.message.AlterClientQuotasRequestData;
import io.kroxylicious.kafka.common.message.AlterClientQuotasResponseData;
import io.kroxylicious.kafka.common.message.AlterConfigsRequestData;
import io.kroxylicious.kafka.common.message.AlterConfigsResponseData;
import io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsRequestData;
import io.kroxylicious.kafka.common.message.AlterPartitionReassignmentsResponseData;
import io.kroxylicious.kafka.common.message.AlterPartitionRequestData;
import io.kroxylicious.kafka.common.message.AlterPartitionResponseData;
import io.kroxylicious.kafka.common.message.AlterReplicaLogDirsRequestData;
import io.kroxylicious.kafka.common.message.AlterReplicaLogDirsResponseData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsResponseData;
import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.BeginQuorumEpochRequestData;
import io.kroxylicious.kafka.common.message.BeginQuorumEpochResponseData;
import io.kroxylicious.kafka.common.message.BrokerHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.BrokerHeartbeatResponseData;
import io.kroxylicious.kafka.common.message.BrokerRegistrationRequestData;
import io.kroxylicious.kafka.common.message.BrokerRegistrationResponseData;
import io.kroxylicious.kafka.common.message.CreateAclsRequestData;
import io.kroxylicious.kafka.common.message.CreateAclsResponseData;
import io.kroxylicious.kafka.common.message.CreateDelegationTokenRequestData;
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
import io.kroxylicious.kafka.common.message.DeleteTopicsRequestData;
import io.kroxylicious.kafka.common.message.DeleteTopicsResponseData;
import io.kroxylicious.kafka.common.message.DescribeAclsRequestData;
import io.kroxylicious.kafka.common.message.DescribeAclsResponseData;
import io.kroxylicious.kafka.common.message.DescribeClientQuotasRequestData;
import io.kroxylicious.kafka.common.message.DescribeClientQuotasResponseData;
import io.kroxylicious.kafka.common.message.DescribeClusterRequestData;
import io.kroxylicious.kafka.common.message.DescribeClusterResponseData;
import io.kroxylicious.kafka.common.message.DescribeConfigsRequestData;
import io.kroxylicious.kafka.common.message.DescribeConfigsResponseData;
import io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.DescribeDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.DescribeGroupsRequestData;
import io.kroxylicious.kafka.common.message.DescribeGroupsResponseData;
import io.kroxylicious.kafka.common.message.DescribeLogDirsRequestData;
import io.kroxylicious.kafka.common.message.DescribeLogDirsResponseData;
import io.kroxylicious.kafka.common.message.DescribeProducersRequestData;
import io.kroxylicious.kafka.common.message.DescribeProducersResponseData;
import io.kroxylicious.kafka.common.message.DescribeQuorumRequestData;
import io.kroxylicious.kafka.common.message.DescribeQuorumResponseData;
import io.kroxylicious.kafka.common.message.DescribeTransactionsRequestData;
import io.kroxylicious.kafka.common.message.DescribeTransactionsResponseData;
import io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.DescribeUserScramCredentialsResponseData;
import io.kroxylicious.kafka.common.message.ElectLeadersRequestData;
import io.kroxylicious.kafka.common.message.ElectLeadersResponseData;
import io.kroxylicious.kafka.common.message.EndQuorumEpochRequestData;
import io.kroxylicious.kafka.common.message.EndQuorumEpochResponseData;
import io.kroxylicious.kafka.common.message.EndTxnRequestData;
import io.kroxylicious.kafka.common.message.EndTxnResponseData;
import io.kroxylicious.kafka.common.message.EnvelopeRequestData;
import io.kroxylicious.kafka.common.message.EnvelopeResponseData;
import io.kroxylicious.kafka.common.message.ExpireDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.ExpireDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.FetchSnapshotRequestData;
import io.kroxylicious.kafka.common.message.FetchSnapshotResponseData;
import io.kroxylicious.kafka.common.message.FindCoordinatorRequestData;
import io.kroxylicious.kafka.common.message.FindCoordinatorResponseData;
import io.kroxylicious.kafka.common.message.HeartbeatRequestData;
import io.kroxylicious.kafka.common.message.HeartbeatResponseData;
import io.kroxylicious.kafka.common.message.IncrementalAlterConfigsRequestData;
import io.kroxylicious.kafka.common.message.IncrementalAlterConfigsResponseData;
import io.kroxylicious.kafka.common.message.InitProducerIdRequestData;
import io.kroxylicious.kafka.common.message.InitProducerIdResponseData;
import io.kroxylicious.kafka.common.message.JoinGroupRequestData;
import io.kroxylicious.kafka.common.message.JoinGroupResponseData;
import io.kroxylicious.kafka.common.message.LeaveGroupRequestData;
import io.kroxylicious.kafka.common.message.LeaveGroupResponseData;
import io.kroxylicious.kafka.common.message.ListGroupsRequestData;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.ListOffsetsResponseData;
import io.kroxylicious.kafka.common.message.ListPartitionReassignmentsRequestData;
import io.kroxylicious.kafka.common.message.ListPartitionReassignmentsResponseData;
import io.kroxylicious.kafka.common.message.ListTransactionsRequestData;
import io.kroxylicious.kafka.common.message.ListTransactionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.OffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.OffsetCommitResponseData;
import io.kroxylicious.kafka.common.message.OffsetDeleteRequestData;
import io.kroxylicious.kafka.common.message.OffsetDeleteResponseData;
import io.kroxylicious.kafka.common.message.OffsetFetchRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchResponseData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceResponseData;
import io.kroxylicious.kafka.common.message.RenewDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.RenewDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.message.SyncGroupRequestData;
import io.kroxylicious.kafka.common.message.SyncGroupResponseData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitResponseData;
import io.kroxylicious.kafka.common.message.UnregisterBrokerRequestData;
import io.kroxylicious.kafka.common.message.UnregisterBrokerResponseData;
import io.kroxylicious.kafka.common.message.UpdateFeaturesRequestData;
import io.kroxylicious.kafka.common.message.UpdateFeaturesResponseData;
import io.kroxylicious.kafka.common.message.VoteRequestData;
import io.kroxylicious.kafka.common.message.VoteResponseData;
import io.kroxylicious.kafka.common.message.WriteTxnMarkersRequestData;
import io.kroxylicious.kafka.common.message.WriteTxnMarkersResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

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
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
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
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.filter.SaslAuthenticateResponseFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeRequestFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeResponseFilter;
import io.kroxylicious.proxy.filter.SyncGroupRequestFilter;
import io.kroxylicious.proxy.filter.SyncGroupResponseFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.UnregisterBrokerRequestFilter;
import io.kroxylicious.proxy.filter.UnregisterBrokerResponseFilter;
import io.kroxylicious.proxy.filter.UpdateFeaturesRequestFilter;
import io.kroxylicious.proxy.filter.UpdateFeaturesResponseFilter;
import io.kroxylicious.proxy.filter.VoteRequestFilter;
import io.kroxylicious.proxy.filter.VoteResponseFilter;
import io.kroxylicious.proxy.filter.WriteTxnMarkersRequestFilter;
import io.kroxylicious.proxy.filter.WriteTxnMarkersResponseFilter;
import io.kroxylicious.proxy.internal.filter.FilterInvoker;

/**
 * Invoker for Filters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
public class SpecificFilterInvoker implements FilterInvoker {

    private final Filter filter;

    public SpecificFilterInvoker(Filter filter) {
        this.filter = filter;
    }

    /**
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
            case ADD_OFFSETS_TO_TXN -> filter instanceof AddOffsetsToTxnRequestFilter addOffsetsToTxnRequestFilter
                    && addOffsetsToTxnRequestFilter.shouldHandleAddOffsetsToTxnRequest(apiVersion);
            case ADD_PARTITIONS_TO_TXN -> filter instanceof AddPartitionsToTxnRequestFilter addPartitionsToTxnRequestFilter
                    && addPartitionsToTxnRequestFilter.shouldHandleAddPartitionsToTxnRequest(apiVersion);
            case ALLOCATE_PRODUCER_IDS -> filter instanceof AllocateProducerIdsRequestFilter allocateProducerIdsRequestFilter
                    && allocateProducerIdsRequestFilter.shouldHandleAllocateProducerIdsRequest(apiVersion);
            case ALTER_CLIENT_QUOTAS -> filter instanceof AlterClientQuotasRequestFilter alterClientQuotasRequestFilter
                    && alterClientQuotasRequestFilter.shouldHandleAlterClientQuotasRequest(apiVersion);
            case ALTER_CONFIGS -> filter instanceof AlterConfigsRequestFilter alterConfigsRequestFilter
                    && alterConfigsRequestFilter.shouldHandleAlterConfigsRequest(apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> filter instanceof AlterPartitionReassignmentsRequestFilter alterPartitionReassignmentsRequestFilter
                    && alterPartitionReassignmentsRequestFilter.shouldHandleAlterPartitionReassignmentsRequest(apiVersion);
            case ALTER_PARTITION -> filter instanceof AlterPartitionRequestFilter alterPartitionRequestFilter
                    && alterPartitionRequestFilter.shouldHandleAlterPartitionRequest(apiVersion);
            case ALTER_REPLICA_LOG_DIRS -> filter instanceof AlterReplicaLogDirsRequestFilter alterReplicaLogDirsRequestFilter
                    && alterReplicaLogDirsRequestFilter.shouldHandleAlterReplicaLogDirsRequest(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> filter instanceof AlterUserScramCredentialsRequestFilter alterUserScramCredentialsRequestFilter
                    && alterUserScramCredentialsRequestFilter.shouldHandleAlterUserScramCredentialsRequest(apiVersion);
            case API_VERSIONS -> filter instanceof ApiVersionsRequestFilter apiVersionsRequestFilter
                    && apiVersionsRequestFilter.shouldHandleApiVersionsRequest(apiVersion);
            case BEGIN_QUORUM_EPOCH -> filter instanceof BeginQuorumEpochRequestFilter beginQuorumEpochRequestFilter
                    && beginQuorumEpochRequestFilter.shouldHandleBeginQuorumEpochRequest(apiVersion);
            case BROKER_HEARTBEAT -> filter instanceof BrokerHeartbeatRequestFilter brokerHeartbeatRequestFilter
                    && brokerHeartbeatRequestFilter.shouldHandleBrokerHeartbeatRequest(apiVersion);
            case BROKER_REGISTRATION -> filter instanceof BrokerRegistrationRequestFilter brokerRegistrationRequestFilter
                    && brokerRegistrationRequestFilter.shouldHandleBrokerRegistrationRequest(apiVersion);
            case CREATE_ACLS -> filter instanceof CreateAclsRequestFilter createAclsRequestFilter && createAclsRequestFilter.shouldHandleCreateAclsRequest(apiVersion);
            case CREATE_DELEGATION_TOKEN -> filter instanceof CreateDelegationTokenRequestFilter createDelegationTokenRequestFilter
                    && createDelegationTokenRequestFilter.shouldHandleCreateDelegationTokenRequest(apiVersion);
            case CREATE_PARTITIONS -> filter instanceof CreatePartitionsRequestFilter createPartitionsRequestFilter
                    && createPartitionsRequestFilter.shouldHandleCreatePartitionsRequest(apiVersion);
            case CREATE_TOPICS -> filter instanceof CreateTopicsRequestFilter createTopicsRequestFilter
                    && createTopicsRequestFilter.shouldHandleCreateTopicsRequest(apiVersion);
            case DELETE_ACLS -> filter instanceof DeleteAclsRequestFilter deleteAclsRequestFilter && deleteAclsRequestFilter.shouldHandleDeleteAclsRequest(apiVersion);
            case DELETE_GROUPS -> filter instanceof DeleteGroupsRequestFilter deleteGroupsRequestFilter
                    && deleteGroupsRequestFilter.shouldHandleDeleteGroupsRequest(apiVersion);
            case DELETE_RECORDS -> filter instanceof DeleteRecordsRequestFilter deleteRecordsRequestFilter
                    && deleteRecordsRequestFilter.shouldHandleDeleteRecordsRequest(apiVersion);
            case DELETE_TOPICS -> filter instanceof DeleteTopicsRequestFilter deleteTopicsRequestFilter
                    && deleteTopicsRequestFilter.shouldHandleDeleteTopicsRequest(apiVersion);
            case DESCRIBE_ACLS -> filter instanceof DescribeAclsRequestFilter describeAclsRequestFilter
                    && describeAclsRequestFilter.shouldHandleDescribeAclsRequest(apiVersion);
            case DESCRIBE_CLIENT_QUOTAS -> filter instanceof DescribeClientQuotasRequestFilter describeClientQuotasRequestFilter
                    && describeClientQuotasRequestFilter.shouldHandleDescribeClientQuotasRequest(apiVersion);
            case DESCRIBE_CLUSTER -> filter instanceof DescribeClusterRequestFilter describeClusterRequestFilter
                    && describeClusterRequestFilter.shouldHandleDescribeClusterRequest(apiVersion);
            case DESCRIBE_CONFIGS -> filter instanceof DescribeConfigsRequestFilter describeConfigsRequestFilter
                    && describeConfigsRequestFilter.shouldHandleDescribeConfigsRequest(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> filter instanceof DescribeDelegationTokenRequestFilter describeDelegationTokenRequestFilter
                    && describeDelegationTokenRequestFilter.shouldHandleDescribeDelegationTokenRequest(apiVersion);
            case DESCRIBE_GROUPS -> filter instanceof DescribeGroupsRequestFilter describeGroupsRequestFilter
                    && describeGroupsRequestFilter.shouldHandleDescribeGroupsRequest(apiVersion);
            case DESCRIBE_LOG_DIRS -> filter instanceof DescribeLogDirsRequestFilter describeLogDirsRequestFilter
                    && describeLogDirsRequestFilter.shouldHandleDescribeLogDirsRequest(apiVersion);
            case DESCRIBE_PRODUCERS -> filter instanceof DescribeProducersRequestFilter describeProducersRequestFilter
                    && describeProducersRequestFilter.shouldHandleDescribeProducersRequest(apiVersion);
            case DESCRIBE_QUORUM -> filter instanceof DescribeQuorumRequestFilter describeQuorumRequestFilter
                    && describeQuorumRequestFilter.shouldHandleDescribeQuorumRequest(apiVersion);
            case DESCRIBE_TRANSACTIONS -> filter instanceof DescribeTransactionsRequestFilter describeTransactionsRequestFilter
                    && describeTransactionsRequestFilter.shouldHandleDescribeTransactionsRequest(apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> filter instanceof DescribeUserScramCredentialsRequestFilter describeUserScramCredentialsRequestFilter
                    && describeUserScramCredentialsRequestFilter.shouldHandleDescribeUserScramCredentialsRequest(apiVersion);
            case ELECT_LEADERS -> filter instanceof ElectLeadersRequestFilter electLeadersRequestFilter
                    && electLeadersRequestFilter.shouldHandleElectLeadersRequest(apiVersion);
            case END_QUORUM_EPOCH -> filter instanceof EndQuorumEpochRequestFilter endQuorumEpochRequestFilter
                    && endQuorumEpochRequestFilter.shouldHandleEndQuorumEpochRequest(apiVersion);
            case END_TXN -> filter instanceof EndTxnRequestFilter endTxnRequestFilter && endTxnRequestFilter.shouldHandleEndTxnRequest(apiVersion);
            case ENVELOPE -> filter instanceof EnvelopeRequestFilter envelopeRequestFilter && envelopeRequestFilter.shouldHandleEnvelopeRequest(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> filter instanceof ExpireDelegationTokenRequestFilter expireDelegationTokenRequestFilter
                    && expireDelegationTokenRequestFilter.shouldHandleExpireDelegationTokenRequest(apiVersion);
            case FETCH -> filter instanceof FetchRequestFilter fetchRequestFilter && fetchRequestFilter.shouldHandleFetchRequest(apiVersion);
            case FETCH_SNAPSHOT -> filter instanceof FetchSnapshotRequestFilter fetchSnapshotRequestFilter
                    && fetchSnapshotRequestFilter.shouldHandleFetchSnapshotRequest(apiVersion);
            case FIND_COORDINATOR -> filter instanceof FindCoordinatorRequestFilter findCoordinatorRequestFilter
                    && findCoordinatorRequestFilter.shouldHandleFindCoordinatorRequest(apiVersion);
            case HEARTBEAT -> filter instanceof HeartbeatRequestFilter heartbeatRequestFilter && heartbeatRequestFilter.shouldHandleHeartbeatRequest(apiVersion);
            case INCREMENTAL_ALTER_CONFIGS -> filter instanceof IncrementalAlterConfigsRequestFilter incrementalAlterConfigsRequestFilter
                    && incrementalAlterConfigsRequestFilter.shouldHandleIncrementalAlterConfigsRequest(apiVersion);
            case INIT_PRODUCER_ID -> filter instanceof InitProducerIdRequestFilter initProducerIdRequestFilter
                    && initProducerIdRequestFilter.shouldHandleInitProducerIdRequest(apiVersion);
            case JOIN_GROUP -> filter instanceof JoinGroupRequestFilter joinGroupRequestFilter && joinGroupRequestFilter.shouldHandleJoinGroupRequest(apiVersion);
            case LEAVE_GROUP -> filter instanceof LeaveGroupRequestFilter leaveGroupRequestFilter && leaveGroupRequestFilter.shouldHandleLeaveGroupRequest(apiVersion);
            case LIST_GROUPS -> filter instanceof ListGroupsRequestFilter listGroupsRequestFilter && listGroupsRequestFilter.shouldHandleListGroupsRequest(apiVersion);
            case LIST_OFFSETS -> filter instanceof ListOffsetsRequestFilter listOffsetsRequestFilter
                    && listOffsetsRequestFilter.shouldHandleListOffsetsRequest(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> filter instanceof ListPartitionReassignmentsRequestFilter listPartitionReassignmentsRequestFilter
                    && listPartitionReassignmentsRequestFilter.shouldHandleListPartitionReassignmentsRequest(apiVersion);
            case LIST_TRANSACTIONS -> filter instanceof ListTransactionsRequestFilter listTransactionsRequestFilter
                    && listTransactionsRequestFilter.shouldHandleListTransactionsRequest(apiVersion);
            case METADATA -> filter instanceof MetadataRequestFilter metadataRequestFilter && metadataRequestFilter.shouldHandleMetadataRequest(apiVersion);
            case OFFSET_COMMIT -> filter instanceof OffsetCommitRequestFilter offsetCommitRequestFilter
                    && offsetCommitRequestFilter.shouldHandleOffsetCommitRequest(apiVersion);
            case OFFSET_DELETE -> filter instanceof OffsetDeleteRequestFilter offsetDeleteRequestFilter
                    && offsetDeleteRequestFilter.shouldHandleOffsetDeleteRequest(apiVersion);
            case OFFSET_FETCH -> filter instanceof OffsetFetchRequestFilter offsetFetchRequestFilter
                    && offsetFetchRequestFilter.shouldHandleOffsetFetchRequest(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH -> filter instanceof OffsetForLeaderEpochRequestFilter offsetForLeaderEpochRequestFilter
                    && offsetForLeaderEpochRequestFilter.shouldHandleOffsetForLeaderEpochRequest(apiVersion);
            case PRODUCE -> filter instanceof ProduceRequestFilter produceRequestFilter && produceRequestFilter.shouldHandleProduceRequest(apiVersion);
            case RENEW_DELEGATION_TOKEN -> filter instanceof RenewDelegationTokenRequestFilter renewDelegationTokenRequestFilter
                    && renewDelegationTokenRequestFilter.shouldHandleRenewDelegationTokenRequest(apiVersion);
            case SASL_AUTHENTICATE -> filter instanceof SaslAuthenticateRequestFilter saslAuthenticateRequestFilter
                    && saslAuthenticateRequestFilter.shouldHandleSaslAuthenticateRequest(apiVersion);
            case SASL_HANDSHAKE -> filter instanceof SaslHandshakeRequestFilter saslHandshakeRequestFilter
                    && saslHandshakeRequestFilter.shouldHandleSaslHandshakeRequest(apiVersion);
            case SYNC_GROUP -> filter instanceof SyncGroupRequestFilter syncGroupRequestFilter && syncGroupRequestFilter.shouldHandleSyncGroupRequest(apiVersion);
            case TXN_OFFSET_COMMIT -> filter instanceof TxnOffsetCommitRequestFilter txnOffsetCommitRequestFilter
                    && txnOffsetCommitRequestFilter.shouldHandleTxnOffsetCommitRequest(apiVersion);
            case UNREGISTER_BROKER -> filter instanceof UnregisterBrokerRequestFilter unregisterBrokerRequestFilter
                    && unregisterBrokerRequestFilter.shouldHandleUnregisterBrokerRequest(apiVersion);
            case UPDATE_FEATURES -> filter instanceof UpdateFeaturesRequestFilter updateFeaturesRequestFilter
                    && updateFeaturesRequestFilter.shouldHandleUpdateFeaturesRequest(apiVersion);
            case VOTE -> filter instanceof VoteRequestFilter voteRequestFilter && voteRequestFilter.shouldHandleVoteRequest(apiVersion);
            case WRITE_TXN_MARKERS -> filter instanceof WriteTxnMarkersRequestFilter writeTxnMarkersRequestFilter
                    && writeTxnMarkersRequestFilter.shouldHandleWriteTxnMarkersRequest(apiVersion);
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
            case ADD_OFFSETS_TO_TXN -> filter instanceof AddOffsetsToTxnResponseFilter addOffsetsToTxnResponseFilter
                    && addOffsetsToTxnResponseFilter.shouldHandleAddOffsetsToTxnResponse(apiVersion);
            case ADD_PARTITIONS_TO_TXN -> filter instanceof AddPartitionsToTxnResponseFilter addPartitionsToTxnResponseFilter
                    && addPartitionsToTxnResponseFilter.shouldHandleAddPartitionsToTxnResponse(apiVersion);
            case ALLOCATE_PRODUCER_IDS -> filter instanceof AllocateProducerIdsResponseFilter allocateProducerIdsResponseFilter
                    && allocateProducerIdsResponseFilter.shouldHandleAllocateProducerIdsResponse(apiVersion);
            case ALTER_CLIENT_QUOTAS -> filter instanceof AlterClientQuotasResponseFilter alterClientQuotasResponseFilter
                    && alterClientQuotasResponseFilter.shouldHandleAlterClientQuotasResponse(apiVersion);
            case ALTER_CONFIGS -> filter instanceof AlterConfigsResponseFilter alterConfigsResponseFilter
                    && alterConfigsResponseFilter.shouldHandleAlterConfigsResponse(apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> filter instanceof AlterPartitionReassignmentsResponseFilter alterPartitionReassignmentsResponseFilter
                    && alterPartitionReassignmentsResponseFilter.shouldHandleAlterPartitionReassignmentsResponse(apiVersion);
            case ALTER_PARTITION -> filter instanceof AlterPartitionResponseFilter alterPartitionResponseFilter
                    && alterPartitionResponseFilter.shouldHandleAlterPartitionResponse(apiVersion);
            case ALTER_REPLICA_LOG_DIRS -> filter instanceof AlterReplicaLogDirsResponseFilter alterReplicaLogDirsResponseFilter
                    && alterReplicaLogDirsResponseFilter.shouldHandleAlterReplicaLogDirsResponse(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> filter instanceof AlterUserScramCredentialsResponseFilter alterUserScramCredentialsResponseFilter
                    && alterUserScramCredentialsResponseFilter.shouldHandleAlterUserScramCredentialsResponse(apiVersion);
            case API_VERSIONS -> filter instanceof ApiVersionsResponseFilter apiVersionsResponseFilter
                    && apiVersionsResponseFilter.shouldHandleApiVersionsResponse(apiVersion);
            case BEGIN_QUORUM_EPOCH -> filter instanceof BeginQuorumEpochResponseFilter beginQuorumEpochResponseFilter
                    && beginQuorumEpochResponseFilter.shouldHandleBeginQuorumEpochResponse(apiVersion);
            case BROKER_HEARTBEAT -> filter instanceof BrokerHeartbeatResponseFilter brokerHeartbeatResponseFilter
                    && brokerHeartbeatResponseFilter.shouldHandleBrokerHeartbeatResponse(apiVersion);
            case BROKER_REGISTRATION -> filter instanceof BrokerRegistrationResponseFilter brokerRegistrationResponseFilter
                    && brokerRegistrationResponseFilter.shouldHandleBrokerRegistrationResponse(apiVersion);
            case CREATE_ACLS -> filter instanceof CreateAclsResponseFilter createAclsResponseFilter
                    && createAclsResponseFilter.shouldHandleCreateAclsResponse(apiVersion);
            case CREATE_DELEGATION_TOKEN -> filter instanceof CreateDelegationTokenResponseFilter createDelegationTokenResponseFilter
                    && createDelegationTokenResponseFilter.shouldHandleCreateDelegationTokenResponse(apiVersion);
            case CREATE_PARTITIONS -> filter instanceof CreatePartitionsResponseFilter createPartitionsResponseFilter
                    && createPartitionsResponseFilter.shouldHandleCreatePartitionsResponse(apiVersion);
            case CREATE_TOPICS -> filter instanceof CreateTopicsResponseFilter createTopicsResponseFilter
                    && createTopicsResponseFilter.shouldHandleCreateTopicsResponse(apiVersion);
            case DELETE_ACLS -> filter instanceof DeleteAclsResponseFilter deleteAclsResponseFilter
                    && deleteAclsResponseFilter.shouldHandleDeleteAclsResponse(apiVersion);
            case DELETE_GROUPS -> filter instanceof DeleteGroupsResponseFilter deleteGroupsResponseFilter
                    && deleteGroupsResponseFilter.shouldHandleDeleteGroupsResponse(apiVersion);
            case DELETE_RECORDS -> filter instanceof DeleteRecordsResponseFilter deleteRecordsResponseFilter
                    && deleteRecordsResponseFilter.shouldHandleDeleteRecordsResponse(apiVersion);
            case DELETE_TOPICS -> filter instanceof DeleteTopicsResponseFilter deleteTopicsResponseFilter
                    && deleteTopicsResponseFilter.shouldHandleDeleteTopicsResponse(apiVersion);
            case DESCRIBE_ACLS -> filter instanceof DescribeAclsResponseFilter describeAclsResponseFilter
                    && describeAclsResponseFilter.shouldHandleDescribeAclsResponse(apiVersion);
            case DESCRIBE_CLIENT_QUOTAS -> filter instanceof DescribeClientQuotasResponseFilter describeClientQuotasResponseFilter
                    && describeClientQuotasResponseFilter.shouldHandleDescribeClientQuotasResponse(apiVersion);
            case DESCRIBE_CLUSTER -> filter instanceof DescribeClusterResponseFilter describeClusterResponseFilter
                    && describeClusterResponseFilter.shouldHandleDescribeClusterResponse(apiVersion);
            case DESCRIBE_CONFIGS -> filter instanceof DescribeConfigsResponseFilter describeConfigsResponseFilter
                    && describeConfigsResponseFilter.shouldHandleDescribeConfigsResponse(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> filter instanceof DescribeDelegationTokenResponseFilter describeDelegationTokenResponseFilter
                    && describeDelegationTokenResponseFilter.shouldHandleDescribeDelegationTokenResponse(apiVersion);
            case DESCRIBE_GROUPS -> filter instanceof DescribeGroupsResponseFilter describeGroupsResponseFilter
                    && describeGroupsResponseFilter.shouldHandleDescribeGroupsResponse(apiVersion);
            case DESCRIBE_LOG_DIRS -> filter instanceof DescribeLogDirsResponseFilter describeLogDirsResponseFilter
                    && describeLogDirsResponseFilter.shouldHandleDescribeLogDirsResponse(apiVersion);
            case DESCRIBE_PRODUCERS -> filter instanceof DescribeProducersResponseFilter describeProducersResponseFilter
                    && describeProducersResponseFilter.shouldHandleDescribeProducersResponse(apiVersion);
            case DESCRIBE_QUORUM -> filter instanceof DescribeQuorumResponseFilter describeQuorumResponseFilter
                    && describeQuorumResponseFilter.shouldHandleDescribeQuorumResponse(apiVersion);
            case DESCRIBE_TRANSACTIONS -> filter instanceof DescribeTransactionsResponseFilter describeTransactionsResponseFilter
                    && describeTransactionsResponseFilter.shouldHandleDescribeTransactionsResponse(apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> filter instanceof DescribeUserScramCredentialsResponseFilter describeUserScramCredentialsResponseFilter
                    && describeUserScramCredentialsResponseFilter.shouldHandleDescribeUserScramCredentialsResponse(apiVersion);
            case ELECT_LEADERS -> filter instanceof ElectLeadersResponseFilter electLeadersResponseFilter
                    && electLeadersResponseFilter.shouldHandleElectLeadersResponse(apiVersion);
            case END_QUORUM_EPOCH -> filter instanceof EndQuorumEpochResponseFilter endQuorumEpochResponseFilter
                    && endQuorumEpochResponseFilter.shouldHandleEndQuorumEpochResponse(apiVersion);
            case END_TXN -> filter instanceof EndTxnResponseFilter endTxnResponseFilter && endTxnResponseFilter.shouldHandleEndTxnResponse(apiVersion);
            case ENVELOPE -> filter instanceof EnvelopeResponseFilter envelopeResponseFilter && envelopeResponseFilter.shouldHandleEnvelopeResponse(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> filter instanceof ExpireDelegationTokenResponseFilter expireDelegationTokenResponseFilter
                    && expireDelegationTokenResponseFilter.shouldHandleExpireDelegationTokenResponse(apiVersion);
            case FETCH -> filter instanceof FetchResponseFilter fetchResponseFilter && fetchResponseFilter.shouldHandleFetchResponse(apiVersion);
            case FETCH_SNAPSHOT -> filter instanceof FetchSnapshotResponseFilter fetchSnapshotResponseFilter
                    && fetchSnapshotResponseFilter.shouldHandleFetchSnapshotResponse(apiVersion);
            case FIND_COORDINATOR -> filter instanceof FindCoordinatorResponseFilter findCoordinatorResponseFilter
                    && findCoordinatorResponseFilter.shouldHandleFindCoordinatorResponse(apiVersion);
            case HEARTBEAT -> filter instanceof HeartbeatResponseFilter heartbeatResponseFilter && heartbeatResponseFilter.shouldHandleHeartbeatResponse(apiVersion);
            case INCREMENTAL_ALTER_CONFIGS -> filter instanceof IncrementalAlterConfigsResponseFilter incrementalAlterConfigsResponseFilter
                    && incrementalAlterConfigsResponseFilter.shouldHandleIncrementalAlterConfigsResponse(apiVersion);
            case INIT_PRODUCER_ID -> filter instanceof InitProducerIdResponseFilter initProducerIdResponseFilter
                    && initProducerIdResponseFilter.shouldHandleInitProducerIdResponse(apiVersion);
            case JOIN_GROUP -> filter instanceof JoinGroupResponseFilter joinGroupResponseFilter && joinGroupResponseFilter.shouldHandleJoinGroupResponse(apiVersion);
            case LEAVE_GROUP -> filter instanceof LeaveGroupResponseFilter leaveGroupResponseFilter
                    && leaveGroupResponseFilter.shouldHandleLeaveGroupResponse(apiVersion);
            case LIST_GROUPS -> filter instanceof ListGroupsResponseFilter listGroupsResponseFilter
                    && listGroupsResponseFilter.shouldHandleListGroupsResponse(apiVersion);
            case LIST_OFFSETS -> filter instanceof ListOffsetsResponseFilter listOffsetsResponseFilter
                    && listOffsetsResponseFilter.shouldHandleListOffsetsResponse(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> filter instanceof ListPartitionReassignmentsResponseFilter listPartitionReassignmentsResponseFilter
                    && listPartitionReassignmentsResponseFilter.shouldHandleListPartitionReassignmentsResponse(apiVersion);
            case LIST_TRANSACTIONS -> filter instanceof ListTransactionsResponseFilter listTransactionsResponseFilter
                    && listTransactionsResponseFilter.shouldHandleListTransactionsResponse(apiVersion);
            case METADATA -> filter instanceof MetadataResponseFilter metadataResponseFilter && metadataResponseFilter.shouldHandleMetadataResponse(apiVersion);
            case OFFSET_COMMIT -> filter instanceof OffsetCommitResponseFilter offsetCommitResponseFilter
                    && offsetCommitResponseFilter.shouldHandleOffsetCommitResponse(apiVersion);
            case OFFSET_DELETE -> filter instanceof OffsetDeleteResponseFilter offsetDeleteResponseFilter
                    && offsetDeleteResponseFilter.shouldHandleOffsetDeleteResponse(apiVersion);
            case OFFSET_FETCH -> filter instanceof OffsetFetchResponseFilter offsetFetchResponseFilter
                    && offsetFetchResponseFilter.shouldHandleOffsetFetchResponse(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH -> filter instanceof OffsetForLeaderEpochResponseFilter offsetForLeaderEpochResponseFilter
                    && offsetForLeaderEpochResponseFilter.shouldHandleOffsetForLeaderEpochResponse(apiVersion);
            case PRODUCE -> filter instanceof ProduceResponseFilter produceResponseFilter && produceResponseFilter.shouldHandleProduceResponse(apiVersion);
            case RENEW_DELEGATION_TOKEN -> filter instanceof RenewDelegationTokenResponseFilter renewDelegationTokenResponseFilter
                    && renewDelegationTokenResponseFilter.shouldHandleRenewDelegationTokenResponse(apiVersion);
            case SASL_AUTHENTICATE -> filter instanceof SaslAuthenticateResponseFilter saslAuthenticateResponseFilter
                    && saslAuthenticateResponseFilter.shouldHandleSaslAuthenticateResponse(apiVersion);
            case SASL_HANDSHAKE -> filter instanceof SaslHandshakeResponseFilter saslHandshakeResponseFilter
                    && saslHandshakeResponseFilter.shouldHandleSaslHandshakeResponse(apiVersion);
            case SYNC_GROUP -> filter instanceof SyncGroupResponseFilter syncGroupResponseFilter && syncGroupResponseFilter.shouldHandleSyncGroupResponse(apiVersion);
            case TXN_OFFSET_COMMIT -> filter instanceof TxnOffsetCommitResponseFilter txnOffsetCommitResponseFilter
                    && txnOffsetCommitResponseFilter.shouldHandleTxnOffsetCommitResponse(apiVersion);
            case UNREGISTER_BROKER -> filter instanceof UnregisterBrokerResponseFilter unregisterBrokerResponseFilter
                    && unregisterBrokerResponseFilter.shouldHandleUnregisterBrokerResponse(apiVersion);
            case UPDATE_FEATURES -> filter instanceof UpdateFeaturesResponseFilter updateFeaturesResponseFilter
                    && updateFeaturesResponseFilter.shouldHandleUpdateFeaturesResponse(apiVersion);
            case VOTE -> filter instanceof VoteResponseFilter voteResponseFilter && voteResponseFilter.shouldHandleVoteResponse(apiVersion);
            case WRITE_TXN_MARKERS -> filter instanceof WriteTxnMarkersResponseFilter writeTxnMarkersResponseFilter
                    && writeTxnMarkersResponseFilter.shouldHandleWriteTxnMarkersResponse(apiVersion);
            default -> throw new IllegalStateException("Unsupported API key " + apiKey);
        };
    }
}
