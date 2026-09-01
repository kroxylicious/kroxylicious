/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.benchmarking.jmh;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ApiMessageType;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
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
import io.kroxylicious.proxy.internal.filter.AddOffsetsToTxnRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AddOffsetsToTxnResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AddPartitionsToTxnRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AddPartitionsToTxnResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AllocateProducerIdsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AllocateProducerIdsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterClientQuotasRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterClientQuotasResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterConfigsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterConfigsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterPartitionReassignmentsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterPartitionReassignmentsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterPartitionRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterPartitionResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterReplicaLogDirsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterReplicaLogDirsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterUserScramCredentialsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.AlterUserScramCredentialsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ApiVersionsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ApiVersionsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.BeginQuorumEpochRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.BeginQuorumEpochResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.BrokerHeartbeatRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.BrokerHeartbeatResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.BrokerRegistrationRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.BrokerRegistrationResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreateAclsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreateAclsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreateDelegationTokenRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreateDelegationTokenResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreatePartitionsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreatePartitionsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreateTopicsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.CreateTopicsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteAclsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteAclsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteGroupsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteGroupsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteRecordsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteRecordsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteTopicsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DeleteTopicsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeAclsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeAclsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeClientQuotasRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeClientQuotasResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeClusterRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeClusterResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeConfigsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeConfigsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeDelegationTokenRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeDelegationTokenResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeGroupsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeGroupsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeLogDirsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeLogDirsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeProducersRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeProducersResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeQuorumRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeQuorumResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeTransactionsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeTransactionsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeUserScramCredentialsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.DescribeUserScramCredentialsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ElectLeadersRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ElectLeadersResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.EndQuorumEpochRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.EndQuorumEpochResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.EndTxnRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.EndTxnResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.EnvelopeRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.EnvelopeResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ExpireDelegationTokenRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ExpireDelegationTokenResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.FetchRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.FetchResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.FetchSnapshotRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.FetchSnapshotResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.FilterInvoker;
import io.kroxylicious.proxy.internal.filter.FilterInvokers;
import io.kroxylicious.proxy.internal.filter.FindCoordinatorRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.FindCoordinatorResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.HeartbeatRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.HeartbeatResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.IncrementalAlterConfigsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.IncrementalAlterConfigsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.InitProducerIdRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.InitProducerIdResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.JoinGroupRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.JoinGroupResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.LeaveGroupRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.LeaveGroupResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListGroupsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListGroupsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListOffsetsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListOffsetsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListPartitionReassignmentsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListPartitionReassignmentsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListTransactionsRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ListTransactionsResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.MetadataRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.MetadataResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetCommitRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetCommitResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetDeleteRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetDeleteResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetFetchRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetFetchResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetForLeaderEpochRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.OffsetForLeaderEpochResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ProduceRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.ProduceResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.RenewDelegationTokenRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.RenewDelegationTokenResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.SaslAuthenticateRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.SaslAuthenticateResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.SaslHandshakeRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.SaslHandshakeResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.SyncGroupRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.SyncGroupResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.TxnOffsetCommitRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.TxnOffsetCommitResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.UnregisterBrokerRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.UnregisterBrokerResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.UpdateFeaturesRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.UpdateFeaturesResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.VoteRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.VoteResponseFilterInvoker;
import io.kroxylicious.proxy.internal.filter.WriteTxnMarkersRequestFilterInvoker;
import io.kroxylicious.proxy.internal.filter.WriteTxnMarkersResponseFilterInvoker;

/**
 * Invoker for Filters that implement any number of Specific Message interfaces (for
 * example {@link AlterConfigsResponseFilter}.
 */
public class ArrayFilterInvoker implements FilterInvoker {

    private static final FilterInvoker[] HANDLE_NOTHING = createHandleNothing();

    private final FilterInvoker[] requestInvokers;
    private final FilterInvoker[] responseInvokers;

    public ArrayFilterInvoker(Filter filter) {
        Map<Integer, FilterInvoker> requestInvokers = new HashMap<>();
        Map<Integer, FilterInvoker> responseInvokers = new HashMap<>();
        if (filter instanceof AddOffsetsToTxnRequestFilter addOffsetsToTxnRequestFilter) {
            requestInvokers.put(25, new AddOffsetsToTxnRequestFilterInvoker(addOffsetsToTxnRequestFilter));
        }
        if (filter instanceof AddOffsetsToTxnResponseFilter addOffsetsToTxnResponseFilter) {
            responseInvokers.put(25, new AddOffsetsToTxnResponseFilterInvoker(addOffsetsToTxnResponseFilter));
        }
        if (filter instanceof AddPartitionsToTxnRequestFilter addPartitionsToTxnRequestFilter) {
            requestInvokers.put(24, new AddPartitionsToTxnRequestFilterInvoker(addPartitionsToTxnRequestFilter));
        }
        if (filter instanceof AddPartitionsToTxnResponseFilter addPartitionsToTxnResponseFilter) {
            responseInvokers.put(24, new AddPartitionsToTxnResponseFilterInvoker(addPartitionsToTxnResponseFilter));
        }
        if (filter instanceof AllocateProducerIdsRequestFilter allocateProducerIdsRequestFilter) {
            requestInvokers.put(67, new AllocateProducerIdsRequestFilterInvoker(allocateProducerIdsRequestFilter));
        }
        if (filter instanceof AllocateProducerIdsResponseFilter allocateProducerIdsResponseFilter) {
            responseInvokers.put(67, new AllocateProducerIdsResponseFilterInvoker(allocateProducerIdsResponseFilter));
        }
        if (filter instanceof AlterClientQuotasRequestFilter alterClientQuotasRequestFilter) {
            requestInvokers.put(49, new AlterClientQuotasRequestFilterInvoker(alterClientQuotasRequestFilter));
        }
        if (filter instanceof AlterClientQuotasResponseFilter alterClientQuotasResponseFilter) {
            responseInvokers.put(49, new AlterClientQuotasResponseFilterInvoker(alterClientQuotasResponseFilter));
        }
        if (filter instanceof AlterConfigsRequestFilter alterConfigsRequestFilter) {
            requestInvokers.put(33, new AlterConfigsRequestFilterInvoker(alterConfigsRequestFilter));
        }
        if (filter instanceof AlterConfigsResponseFilter alterConfigsResponseFilter) {
            responseInvokers.put(33, new AlterConfigsResponseFilterInvoker(alterConfigsResponseFilter));
        }
        if (filter instanceof AlterPartitionReassignmentsRequestFilter alterPartitionReassignmentsRequestFilter) {
            requestInvokers.put(45, new AlterPartitionReassignmentsRequestFilterInvoker(alterPartitionReassignmentsRequestFilter));
        }
        if (filter instanceof AlterPartitionReassignmentsResponseFilter alterPartitionReassignmentsResponseFilter) {
            responseInvokers.put(45, new AlterPartitionReassignmentsResponseFilterInvoker(alterPartitionReassignmentsResponseFilter));
        }
        if (filter instanceof AlterPartitionRequestFilter alterPartitionRequestFilter) {
            requestInvokers.put(56, new AlterPartitionRequestFilterInvoker(alterPartitionRequestFilter));
        }
        if (filter instanceof AlterPartitionResponseFilter alterPartitionResponseFilter) {
            responseInvokers.put(56, new AlterPartitionResponseFilterInvoker(alterPartitionResponseFilter));
        }
        if (filter instanceof AlterReplicaLogDirsRequestFilter alterReplicaLogDirsRequestFilter) {
            requestInvokers.put(34, new AlterReplicaLogDirsRequestFilterInvoker(alterReplicaLogDirsRequestFilter));
        }
        if (filter instanceof AlterReplicaLogDirsResponseFilter alterReplicaLogDirsResponseFilter) {
            responseInvokers.put(34, new AlterReplicaLogDirsResponseFilterInvoker(alterReplicaLogDirsResponseFilter));
        }
        if (filter instanceof AlterUserScramCredentialsRequestFilter alterUserScramCredentialsRequestFilter) {
            requestInvokers.put(51, new AlterUserScramCredentialsRequestFilterInvoker(alterUserScramCredentialsRequestFilter));
        }
        if (filter instanceof AlterUserScramCredentialsResponseFilter alterUserScramCredentialsResponseFilter) {
            responseInvokers.put(51, new AlterUserScramCredentialsResponseFilterInvoker(alterUserScramCredentialsResponseFilter));
        }
        if (filter instanceof ApiVersionsRequestFilter apiVersionsRequestFilter) {
            requestInvokers.put(18, new ApiVersionsRequestFilterInvoker(apiVersionsRequestFilter));
        }
        if (filter instanceof ApiVersionsResponseFilter apiVersionsResponseFilter) {
            responseInvokers.put(18, new ApiVersionsResponseFilterInvoker(apiVersionsResponseFilter));
        }
        if (filter instanceof BeginQuorumEpochRequestFilter beginQuorumEpochRequestFilter) {
            requestInvokers.put(53, new BeginQuorumEpochRequestFilterInvoker(beginQuorumEpochRequestFilter));
        }
        if (filter instanceof BeginQuorumEpochResponseFilter beginQuorumEpochResponseFilter) {
            responseInvokers.put(53, new BeginQuorumEpochResponseFilterInvoker(beginQuorumEpochResponseFilter));
        }
        if (filter instanceof BrokerHeartbeatRequestFilter brokerHeartbeatRequestFilter) {
            requestInvokers.put(63, new BrokerHeartbeatRequestFilterInvoker(brokerHeartbeatRequestFilter));
        }
        if (filter instanceof BrokerHeartbeatResponseFilter brokerHeartbeatResponseFilter) {
            responseInvokers.put(63, new BrokerHeartbeatResponseFilterInvoker(brokerHeartbeatResponseFilter));
        }
        if (filter instanceof BrokerRegistrationRequestFilter brokerRegistrationRequestFilter) {
            requestInvokers.put(62, new BrokerRegistrationRequestFilterInvoker(brokerRegistrationRequestFilter));
        }
        if (filter instanceof BrokerRegistrationResponseFilter brokerRegistrationResponseFilter) {
            responseInvokers.put(62, new BrokerRegistrationResponseFilterInvoker(brokerRegistrationResponseFilter));
        }
        if (filter instanceof CreateAclsRequestFilter createAclsRequestFilter) {
            requestInvokers.put(30, new CreateAclsRequestFilterInvoker(createAclsRequestFilter));
        }
        if (filter instanceof CreateAclsResponseFilter createAclsResponseFilter) {
            responseInvokers.put(30, new CreateAclsResponseFilterInvoker(createAclsResponseFilter));
        }
        if (filter instanceof CreateDelegationTokenRequestFilter createDelegationTokenRequestFilter) {
            requestInvokers.put(38, new CreateDelegationTokenRequestFilterInvoker(createDelegationTokenRequestFilter));
        }
        if (filter instanceof CreateDelegationTokenResponseFilter createDelegationTokenResponseFilter) {
            responseInvokers.put(38, new CreateDelegationTokenResponseFilterInvoker(createDelegationTokenResponseFilter));
        }
        if (filter instanceof CreatePartitionsRequestFilter createPartitionsRequestFilter) {
            requestInvokers.put(37, new CreatePartitionsRequestFilterInvoker(createPartitionsRequestFilter));
        }
        if (filter instanceof CreatePartitionsResponseFilter createPartitionsResponseFilter) {
            responseInvokers.put(37, new CreatePartitionsResponseFilterInvoker(createPartitionsResponseFilter));
        }
        if (filter instanceof CreateTopicsRequestFilter createTopicsRequestFilter) {
            requestInvokers.put(19, new CreateTopicsRequestFilterInvoker(createTopicsRequestFilter));
        }
        if (filter instanceof CreateTopicsResponseFilter createTopicsResponseFilter) {
            responseInvokers.put(19, new CreateTopicsResponseFilterInvoker(createTopicsResponseFilter));
        }
        if (filter instanceof DeleteAclsRequestFilter deleteAclsRequestFilter) {
            requestInvokers.put(31, new DeleteAclsRequestFilterInvoker(deleteAclsRequestFilter));
        }
        if (filter instanceof DeleteAclsResponseFilter deleteAclsResponseFilter) {
            responseInvokers.put(31, new DeleteAclsResponseFilterInvoker(deleteAclsResponseFilter));
        }
        if (filter instanceof DeleteGroupsRequestFilter deleteGroupsRequestFilter) {
            requestInvokers.put(42, new DeleteGroupsRequestFilterInvoker(deleteGroupsRequestFilter));
        }
        if (filter instanceof DeleteGroupsResponseFilter deleteGroupsResponseFilter) {
            responseInvokers.put(42, new DeleteGroupsResponseFilterInvoker(deleteGroupsResponseFilter));
        }
        if (filter instanceof DeleteRecordsRequestFilter deleteRecordsRequestFilter) {
            requestInvokers.put(21, new DeleteRecordsRequestFilterInvoker(deleteRecordsRequestFilter));
        }
        if (filter instanceof DeleteRecordsResponseFilter deleteRecordsResponseFilter) {
            responseInvokers.put(21, new DeleteRecordsResponseFilterInvoker(deleteRecordsResponseFilter));
        }
        if (filter instanceof DeleteTopicsRequestFilter deleteTopicsRequestFilter) {
            requestInvokers.put(20, new DeleteTopicsRequestFilterInvoker(deleteTopicsRequestFilter));
        }
        if (filter instanceof DeleteTopicsResponseFilter deleteTopicsResponseFilter) {
            responseInvokers.put(20, new DeleteTopicsResponseFilterInvoker(deleteTopicsResponseFilter));
        }
        if (filter instanceof DescribeAclsRequestFilter describeAclsRequestFilter) {
            requestInvokers.put(29, new DescribeAclsRequestFilterInvoker(describeAclsRequestFilter));
        }
        if (filter instanceof DescribeAclsResponseFilter describeAclsResponseFilter) {
            responseInvokers.put(29, new DescribeAclsResponseFilterInvoker(describeAclsResponseFilter));
        }
        if (filter instanceof DescribeClientQuotasRequestFilter describeClientQuotasRequestFilter) {
            requestInvokers.put(48, new DescribeClientQuotasRequestFilterInvoker(describeClientQuotasRequestFilter));
        }
        if (filter instanceof DescribeClientQuotasResponseFilter describeClientQuotasResponseFilter) {
            responseInvokers.put(48, new DescribeClientQuotasResponseFilterInvoker(describeClientQuotasResponseFilter));
        }
        if (filter instanceof DescribeClusterRequestFilter describeClusterRequestFilter) {
            requestInvokers.put(60, new DescribeClusterRequestFilterInvoker(describeClusterRequestFilter));
        }
        if (filter instanceof DescribeClusterResponseFilter describeClusterResponseFilter) {
            responseInvokers.put(60, new DescribeClusterResponseFilterInvoker(describeClusterResponseFilter));
        }
        if (filter instanceof DescribeConfigsRequestFilter describeConfigsRequestFilter) {
            requestInvokers.put(32, new DescribeConfigsRequestFilterInvoker(describeConfigsRequestFilter));
        }
        if (filter instanceof DescribeConfigsResponseFilter describeConfigsResponseFilter) {
            responseInvokers.put(32, new DescribeConfigsResponseFilterInvoker(describeConfigsResponseFilter));
        }
        if (filter instanceof DescribeDelegationTokenRequestFilter describeDelegationTokenRequestFilter) {
            requestInvokers.put(41, new DescribeDelegationTokenRequestFilterInvoker(describeDelegationTokenRequestFilter));
        }
        if (filter instanceof DescribeDelegationTokenResponseFilter describeDelegationTokenResponseFilter) {
            responseInvokers.put(41, new DescribeDelegationTokenResponseFilterInvoker(describeDelegationTokenResponseFilter));
        }
        if (filter instanceof DescribeGroupsRequestFilter describeGroupsRequestFilter) {
            requestInvokers.put(15, new DescribeGroupsRequestFilterInvoker(describeGroupsRequestFilter));
        }
        if (filter instanceof DescribeGroupsResponseFilter describeGroupsResponseFilter) {
            responseInvokers.put(15, new DescribeGroupsResponseFilterInvoker(describeGroupsResponseFilter));
        }
        if (filter instanceof DescribeLogDirsRequestFilter describeLogDirsRequestFilter) {
            requestInvokers.put(35, new DescribeLogDirsRequestFilterInvoker(describeLogDirsRequestFilter));
        }
        if (filter instanceof DescribeLogDirsResponseFilter describeLogDirsResponseFilter) {
            responseInvokers.put(35, new DescribeLogDirsResponseFilterInvoker(describeLogDirsResponseFilter));
        }
        if (filter instanceof DescribeProducersRequestFilter describeProducersRequestFilter) {
            requestInvokers.put(61, new DescribeProducersRequestFilterInvoker(describeProducersRequestFilter));
        }
        if (filter instanceof DescribeProducersResponseFilter describeProducersResponseFilter) {
            responseInvokers.put(61, new DescribeProducersResponseFilterInvoker(describeProducersResponseFilter));
        }
        if (filter instanceof DescribeQuorumRequestFilter describeQuorumRequestFilter) {
            requestInvokers.put(55, new DescribeQuorumRequestFilterInvoker(describeQuorumRequestFilter));
        }
        if (filter instanceof DescribeQuorumResponseFilter describeQuorumResponseFilter) {
            responseInvokers.put(55, new DescribeQuorumResponseFilterInvoker(describeQuorumResponseFilter));
        }
        if (filter instanceof DescribeTransactionsRequestFilter describeTransactionsRequestFilter) {
            requestInvokers.put(65, new DescribeTransactionsRequestFilterInvoker(describeTransactionsRequestFilter));
        }
        if (filter instanceof DescribeTransactionsResponseFilter describeTransactionsResponseFilter) {
            responseInvokers.put(65, new DescribeTransactionsResponseFilterInvoker(describeTransactionsResponseFilter));
        }
        if (filter instanceof DescribeUserScramCredentialsRequestFilter describeUserScramCredentialsRequestFilter) {
            requestInvokers.put(50, new DescribeUserScramCredentialsRequestFilterInvoker(describeUserScramCredentialsRequestFilter));
        }
        if (filter instanceof DescribeUserScramCredentialsResponseFilter describeUserScramCredentialsResponseFilter) {
            responseInvokers.put(50, new DescribeUserScramCredentialsResponseFilterInvoker(describeUserScramCredentialsResponseFilter));
        }
        if (filter instanceof ElectLeadersRequestFilter electLeadersRequestFilter) {
            requestInvokers.put(43, new ElectLeadersRequestFilterInvoker(electLeadersRequestFilter));
        }
        if (filter instanceof ElectLeadersResponseFilter electLeadersResponseFilter) {
            responseInvokers.put(43, new ElectLeadersResponseFilterInvoker(electLeadersResponseFilter));
        }
        if (filter instanceof EndQuorumEpochRequestFilter endQuorumEpochRequestFilter) {
            requestInvokers.put(54, new EndQuorumEpochRequestFilterInvoker(endQuorumEpochRequestFilter));
        }
        if (filter instanceof EndQuorumEpochResponseFilter endQuorumEpochResponseFilter) {
            responseInvokers.put(54, new EndQuorumEpochResponseFilterInvoker(endQuorumEpochResponseFilter));
        }
        if (filter instanceof EndTxnRequestFilter endTxnRequestFilter) {
            requestInvokers.put(26, new EndTxnRequestFilterInvoker(endTxnRequestFilter));
        }
        if (filter instanceof EndTxnResponseFilter endTxnResponseFilter) {
            responseInvokers.put(26, new EndTxnResponseFilterInvoker(endTxnResponseFilter));
        }
        if (filter instanceof EnvelopeRequestFilter envelopeRequestFilter) {
            requestInvokers.put(58, new EnvelopeRequestFilterInvoker(envelopeRequestFilter));
        }
        if (filter instanceof EnvelopeResponseFilter envelopeResponseFilter) {
            responseInvokers.put(58, new EnvelopeResponseFilterInvoker(envelopeResponseFilter));
        }
        if (filter instanceof ExpireDelegationTokenRequestFilter expireDelegationTokenRequestFilter) {
            requestInvokers.put(40, new ExpireDelegationTokenRequestFilterInvoker(expireDelegationTokenRequestFilter));
        }
        if (filter instanceof ExpireDelegationTokenResponseFilter expireDelegationTokenResponseFilter) {
            responseInvokers.put(40, new ExpireDelegationTokenResponseFilterInvoker(expireDelegationTokenResponseFilter));
        }
        if (filter instanceof FetchRequestFilter fetchRequestFilter) {
            requestInvokers.put(1, new FetchRequestFilterInvoker(fetchRequestFilter));
        }
        if (filter instanceof FetchResponseFilter fetchResponseFilter) {
            responseInvokers.put(1, new FetchResponseFilterInvoker(fetchResponseFilter));
        }
        if (filter instanceof FetchSnapshotRequestFilter fetchSnapshotRequestFilter) {
            requestInvokers.put(59, new FetchSnapshotRequestFilterInvoker(fetchSnapshotRequestFilter));
        }
        if (filter instanceof FetchSnapshotResponseFilter fetchSnapshotResponseFilter) {
            responseInvokers.put(59, new FetchSnapshotResponseFilterInvoker(fetchSnapshotResponseFilter));
        }
        if (filter instanceof FindCoordinatorRequestFilter findCoordinatorRequestFilter) {
            requestInvokers.put(10, new FindCoordinatorRequestFilterInvoker(findCoordinatorRequestFilter));
        }
        if (filter instanceof FindCoordinatorResponseFilter findCoordinatorResponseFilter) {
            responseInvokers.put(10, new FindCoordinatorResponseFilterInvoker(findCoordinatorResponseFilter));
        }
        if (filter instanceof HeartbeatRequestFilter heartbeatRequestFilter) {
            requestInvokers.put(12, new HeartbeatRequestFilterInvoker(heartbeatRequestFilter));
        }
        if (filter instanceof HeartbeatResponseFilter heartbeatResponseFilter) {
            responseInvokers.put(12, new HeartbeatResponseFilterInvoker(heartbeatResponseFilter));
        }
        if (filter instanceof IncrementalAlterConfigsRequestFilter incrementalAlterConfigsRequestFilter) {
            requestInvokers.put(44, new IncrementalAlterConfigsRequestFilterInvoker(incrementalAlterConfigsRequestFilter));
        }
        if (filter instanceof IncrementalAlterConfigsResponseFilter incrementalAlterConfigsResponseFilter) {
            responseInvokers.put(44, new IncrementalAlterConfigsResponseFilterInvoker(incrementalAlterConfigsResponseFilter));
        }
        if (filter instanceof InitProducerIdRequestFilter initProducerIdRequestFilter) {
            requestInvokers.put(22, new InitProducerIdRequestFilterInvoker(initProducerIdRequestFilter));
        }
        if (filter instanceof InitProducerIdResponseFilter initProducerIdResponseFilter) {
            responseInvokers.put(22, new InitProducerIdResponseFilterInvoker(initProducerIdResponseFilter));
        }
        if (filter instanceof JoinGroupRequestFilter joinGroupRequestFilter) {
            requestInvokers.put(11, new JoinGroupRequestFilterInvoker(joinGroupRequestFilter));
        }
        if (filter instanceof JoinGroupResponseFilter joinGroupResponseFilter) {
            responseInvokers.put(11, new JoinGroupResponseFilterInvoker(joinGroupResponseFilter));
        }
        if (filter instanceof LeaveGroupRequestFilter leaveGroupRequestFilter) {
            requestInvokers.put(13, new LeaveGroupRequestFilterInvoker(leaveGroupRequestFilter));
        }
        if (filter instanceof LeaveGroupResponseFilter leaveGroupResponseFilter) {
            responseInvokers.put(13, new LeaveGroupResponseFilterInvoker(leaveGroupResponseFilter));
        }
        if (filter instanceof ListGroupsRequestFilter listGroupsRequestFilter) {
            requestInvokers.put(16, new ListGroupsRequestFilterInvoker(listGroupsRequestFilter));
        }
        if (filter instanceof ListGroupsResponseFilter listGroupsResponseFilter) {
            responseInvokers.put(16, new ListGroupsResponseFilterInvoker(listGroupsResponseFilter));
        }
        if (filter instanceof ListOffsetsRequestFilter listOffsetsRequestFilter) {
            requestInvokers.put(2, new ListOffsetsRequestFilterInvoker(listOffsetsRequestFilter));
        }
        if (filter instanceof ListOffsetsResponseFilter listOffsetsResponseFilter) {
            responseInvokers.put(2, new ListOffsetsResponseFilterInvoker(listOffsetsResponseFilter));
        }
        if (filter instanceof ListPartitionReassignmentsRequestFilter listPartitionReassignmentsRequestFilter) {
            requestInvokers.put(46, new ListPartitionReassignmentsRequestFilterInvoker(listPartitionReassignmentsRequestFilter));
        }
        if (filter instanceof ListPartitionReassignmentsResponseFilter listPartitionReassignmentsResponseFilter) {
            responseInvokers.put(46, new ListPartitionReassignmentsResponseFilterInvoker(listPartitionReassignmentsResponseFilter));
        }
        if (filter instanceof ListTransactionsRequestFilter listTransactionsRequestFilter) {
            requestInvokers.put(66, new ListTransactionsRequestFilterInvoker(listTransactionsRequestFilter));
        }
        if (filter instanceof ListTransactionsResponseFilter listTransactionsResponseFilter) {
            responseInvokers.put(66, new ListTransactionsResponseFilterInvoker(listTransactionsResponseFilter));
        }
        if (filter instanceof MetadataRequestFilter metadataRequestFilter) {
            requestInvokers.put(3, new MetadataRequestFilterInvoker(metadataRequestFilter));
        }
        if (filter instanceof MetadataResponseFilter metadataResponseFilter) {
            responseInvokers.put(3, new MetadataResponseFilterInvoker(metadataResponseFilter));
        }
        if (filter instanceof OffsetCommitRequestFilter offsetCommitRequestFilter) {
            requestInvokers.put(8, new OffsetCommitRequestFilterInvoker(offsetCommitRequestFilter));
        }
        if (filter instanceof OffsetCommitResponseFilter offsetCommitResponseFilter) {
            responseInvokers.put(8, new OffsetCommitResponseFilterInvoker(offsetCommitResponseFilter));
        }
        if (filter instanceof OffsetDeleteRequestFilter offsetDeleteRequestFilter) {
            requestInvokers.put(47, new OffsetDeleteRequestFilterInvoker(offsetDeleteRequestFilter));
        }
        if (filter instanceof OffsetDeleteResponseFilter offsetDeleteResponseFilter) {
            responseInvokers.put(47, new OffsetDeleteResponseFilterInvoker(offsetDeleteResponseFilter));
        }
        if (filter instanceof OffsetFetchRequestFilter offsetFetchRequestFilter) {
            requestInvokers.put(9, new OffsetFetchRequestFilterInvoker(offsetFetchRequestFilter));
        }
        if (filter instanceof OffsetFetchResponseFilter offsetFetchResponseFilter) {
            responseInvokers.put(9, new OffsetFetchResponseFilterInvoker(offsetFetchResponseFilter));
        }
        if (filter instanceof OffsetForLeaderEpochRequestFilter offsetForLeaderEpochRequestFilter) {
            requestInvokers.put(23, new OffsetForLeaderEpochRequestFilterInvoker(offsetForLeaderEpochRequestFilter));
        }
        if (filter instanceof OffsetForLeaderEpochResponseFilter offsetForLeaderEpochResponseFilter) {
            responseInvokers.put(23, new OffsetForLeaderEpochResponseFilterInvoker(offsetForLeaderEpochResponseFilter));
        }
        if (filter instanceof ProduceRequestFilter produceRequestFilter) {
            requestInvokers.put(0, new ProduceRequestFilterInvoker(produceRequestFilter));
        }
        if (filter instanceof ProduceResponseFilter produceResponseFilter) {
            responseInvokers.put(0, new ProduceResponseFilterInvoker(produceResponseFilter));
        }
        if (filter instanceof RenewDelegationTokenRequestFilter renewDelegationTokenRequestFilter) {
            requestInvokers.put(39, new RenewDelegationTokenRequestFilterInvoker(renewDelegationTokenRequestFilter));
        }
        if (filter instanceof RenewDelegationTokenResponseFilter renewDelegationTokenResponseFilter) {
            responseInvokers.put(39, new RenewDelegationTokenResponseFilterInvoker(renewDelegationTokenResponseFilter));
        }
        if (filter instanceof SaslAuthenticateRequestFilter saslAuthenticateRequestFilter) {
            requestInvokers.put(36, new SaslAuthenticateRequestFilterInvoker(saslAuthenticateRequestFilter));
        }
        if (filter instanceof SaslAuthenticateResponseFilter saslAuthenticateResponseFilter) {
            responseInvokers.put(36, new SaslAuthenticateResponseFilterInvoker(saslAuthenticateResponseFilter));
        }
        if (filter instanceof SaslHandshakeRequestFilter saslHandshakeRequestFilter) {
            requestInvokers.put(17, new SaslHandshakeRequestFilterInvoker(saslHandshakeRequestFilter));
        }
        if (filter instanceof SaslHandshakeResponseFilter saslHandshakeResponseFilter) {
            responseInvokers.put(17, new SaslHandshakeResponseFilterInvoker(saslHandshakeResponseFilter));
        }
        if (filter instanceof SyncGroupRequestFilter syncGroupRequestFilter) {
            requestInvokers.put(14, new SyncGroupRequestFilterInvoker(syncGroupRequestFilter));
        }
        if (filter instanceof SyncGroupResponseFilter syncGroupResponseFilter) {
            responseInvokers.put(14, new SyncGroupResponseFilterInvoker(syncGroupResponseFilter));
        }
        if (filter instanceof TxnOffsetCommitRequestFilter txnOffsetCommitRequestFilter) {
            requestInvokers.put(28, new TxnOffsetCommitRequestFilterInvoker(txnOffsetCommitRequestFilter));
        }
        if (filter instanceof TxnOffsetCommitResponseFilter txnOffsetCommitResponseFilter) {
            responseInvokers.put(28, new TxnOffsetCommitResponseFilterInvoker(txnOffsetCommitResponseFilter));
        }
        if (filter instanceof UnregisterBrokerRequestFilter unregisterBrokerRequestFilter) {
            requestInvokers.put(64, new UnregisterBrokerRequestFilterInvoker(unregisterBrokerRequestFilter));
        }
        if (filter instanceof UnregisterBrokerResponseFilter unregisterBrokerResponseFilter) {
            responseInvokers.put(64, new UnregisterBrokerResponseFilterInvoker(unregisterBrokerResponseFilter));
        }
        if (filter instanceof UpdateFeaturesRequestFilter updateFeaturesRequestFilter) {
            requestInvokers.put(57, new UpdateFeaturesRequestFilterInvoker(updateFeaturesRequestFilter));
        }
        if (filter instanceof UpdateFeaturesResponseFilter updateFeaturesResponseFilter) {
            responseInvokers.put(57, new UpdateFeaturesResponseFilterInvoker(updateFeaturesResponseFilter));
        }
        if (filter instanceof VoteRequestFilter voteRequestFilter) {
            requestInvokers.put(52, new VoteRequestFilterInvoker(voteRequestFilter));
        }
        if (filter instanceof VoteResponseFilter voteResponseFilter) {
            responseInvokers.put(52, new VoteResponseFilterInvoker(voteResponseFilter));
        }
        if (filter instanceof WriteTxnMarkersRequestFilter writeTxnMarkersRequestFilter) {
            requestInvokers.put(27, new WriteTxnMarkersRequestFilterInvoker(writeTxnMarkersRequestFilter));
        }
        if (filter instanceof WriteTxnMarkersResponseFilter writeTxnMarkersResponseFilter) {
            responseInvokers.put(27, new WriteTxnMarkersResponseFilterInvoker(writeTxnMarkersResponseFilter));
        }
        this.requestInvokers = createFrom(requestInvokers);
        this.responseInvokers = createFrom(responseInvokers);
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
        FilterInvoker invoker = requestInvokers[apiKey.id];
        return invoker.onRequest(apiKey, apiVersion, header, body, filterContext);
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
        FilterInvoker invoker = responseInvokers[apiKey.id];
        return invoker.onResponse(apiKey, apiVersion, header, body, filterContext);
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
        FilterInvoker invoker = requestInvokers[apiKey.id];
        return invoker.shouldHandleRequest(apiKey, apiVersion);
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
        FilterInvoker invoker = responseInvokers[apiKey.id];
        return invoker.shouldHandleResponse(apiKey, apiVersion);
    }

    private static FilterInvoker[] createHandleNothing() {
        FilterInvoker[] filterInvokers = emptyInvokerArraySizedForMessageTypes();
        Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).forEach(value -> {
            filterInvokers[value] = FilterInvokers.handleNothingInvoker();
        });
        return filterInvokers;
    }

    private static FilterInvoker[] createFrom(Map<Integer, FilterInvoker> filterInvokersByApiMessageId) {
        if (filterInvokersByApiMessageId.isEmpty()) {
            return HANDLE_NOTHING;
        }
        FilterInvoker[] filterInvokers = emptyInvokerArraySizedForMessageTypes();
        Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).forEach(value -> {
            filterInvokers[value] = filterInvokersByApiMessageId.getOrDefault(value, FilterInvokers.handleNothingInvoker());
        });
        return filterInvokers;
    }

    private static FilterInvoker[] emptyInvokerArraySizedForMessageTypes() {
        OptionalInt maybeMaxId = Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).max();
        if (maybeMaxId.isEmpty()) {
            throw new IllegalStateException("no maximum id found");
        }
        int arraySize = maybeMaxId.getAsInt() + 1;
        return new FilterInvoker[arraySize];
    }

}
