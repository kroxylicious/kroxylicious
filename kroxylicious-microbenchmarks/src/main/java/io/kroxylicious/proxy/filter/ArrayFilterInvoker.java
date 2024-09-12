/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

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
        if (filter instanceof AddOffsetsToTxnRequestFilter) {
            requestInvokers.put(25, new AddOffsetsToTxnRequestFilterInvoker((AddOffsetsToTxnRequestFilter) filter));
        }
        if (filter instanceof AddOffsetsToTxnResponseFilter) {
            responseInvokers.put(25, new AddOffsetsToTxnResponseFilterInvoker((AddOffsetsToTxnResponseFilter) filter));
        }
        if (filter instanceof AddPartitionsToTxnRequestFilter) {
            requestInvokers.put(24, new AddPartitionsToTxnRequestFilterInvoker((AddPartitionsToTxnRequestFilter) filter));
        }
        if (filter instanceof AddPartitionsToTxnResponseFilter) {
            responseInvokers.put(24, new AddPartitionsToTxnResponseFilterInvoker((AddPartitionsToTxnResponseFilter) filter));
        }
        if (filter instanceof AllocateProducerIdsRequestFilter) {
            requestInvokers.put(67, new AllocateProducerIdsRequestFilterInvoker((AllocateProducerIdsRequestFilter) filter));
        }
        if (filter instanceof AllocateProducerIdsResponseFilter) {
            responseInvokers.put(67, new AllocateProducerIdsResponseFilterInvoker((AllocateProducerIdsResponseFilter) filter));
        }
        if (filter instanceof AlterClientQuotasRequestFilter) {
            requestInvokers.put(49, new AlterClientQuotasRequestFilterInvoker((AlterClientQuotasRequestFilter) filter));
        }
        if (filter instanceof AlterClientQuotasResponseFilter) {
            responseInvokers.put(49, new AlterClientQuotasResponseFilterInvoker((AlterClientQuotasResponseFilter) filter));
        }
        if (filter instanceof AlterConfigsRequestFilter) {
            requestInvokers.put(33, new AlterConfigsRequestFilterInvoker((AlterConfigsRequestFilter) filter));
        }
        if (filter instanceof AlterConfigsResponseFilter) {
            responseInvokers.put(33, new AlterConfigsResponseFilterInvoker((AlterConfigsResponseFilter) filter));
        }
        if (filter instanceof AlterPartitionReassignmentsRequestFilter) {
            requestInvokers.put(45, new AlterPartitionReassignmentsRequestFilterInvoker((AlterPartitionReassignmentsRequestFilter) filter));
        }
        if (filter instanceof AlterPartitionReassignmentsResponseFilter) {
            responseInvokers.put(45, new AlterPartitionReassignmentsResponseFilterInvoker((AlterPartitionReassignmentsResponseFilter) filter));
        }
        if (filter instanceof AlterPartitionRequestFilter) {
            requestInvokers.put(56, new AlterPartitionRequestFilterInvoker((AlterPartitionRequestFilter) filter));
        }
        if (filter instanceof AlterPartitionResponseFilter) {
            responseInvokers.put(56, new AlterPartitionResponseFilterInvoker((AlterPartitionResponseFilter) filter));
        }
        if (filter instanceof AlterReplicaLogDirsRequestFilter) {
            requestInvokers.put(34, new AlterReplicaLogDirsRequestFilterInvoker((AlterReplicaLogDirsRequestFilter) filter));
        }
        if (filter instanceof AlterReplicaLogDirsResponseFilter) {
            responseInvokers.put(34, new AlterReplicaLogDirsResponseFilterInvoker((AlterReplicaLogDirsResponseFilter) filter));
        }
        if (filter instanceof AlterUserScramCredentialsRequestFilter) {
            requestInvokers.put(51, new AlterUserScramCredentialsRequestFilterInvoker((AlterUserScramCredentialsRequestFilter) filter));
        }
        if (filter instanceof AlterUserScramCredentialsResponseFilter) {
            responseInvokers.put(51, new AlterUserScramCredentialsResponseFilterInvoker((AlterUserScramCredentialsResponseFilter) filter));
        }
        if (filter instanceof ApiVersionsRequestFilter) {
            requestInvokers.put(18, new ApiVersionsRequestFilterInvoker((ApiVersionsRequestFilter) filter));
        }
        if (filter instanceof ApiVersionsResponseFilter) {
            responseInvokers.put(18, new ApiVersionsResponseFilterInvoker((ApiVersionsResponseFilter) filter));
        }
        if (filter instanceof BeginQuorumEpochRequestFilter) {
            requestInvokers.put(53, new BeginQuorumEpochRequestFilterInvoker((BeginQuorumEpochRequestFilter) filter));
        }
        if (filter instanceof BeginQuorumEpochResponseFilter) {
            responseInvokers.put(53, new BeginQuorumEpochResponseFilterInvoker((BeginQuorumEpochResponseFilter) filter));
        }
        if (filter instanceof BrokerHeartbeatRequestFilter) {
            requestInvokers.put(63, new BrokerHeartbeatRequestFilterInvoker((BrokerHeartbeatRequestFilter) filter));
        }
        if (filter instanceof BrokerHeartbeatResponseFilter) {
            responseInvokers.put(63, new BrokerHeartbeatResponseFilterInvoker((BrokerHeartbeatResponseFilter) filter));
        }
        if (filter instanceof BrokerRegistrationRequestFilter) {
            requestInvokers.put(62, new BrokerRegistrationRequestFilterInvoker((BrokerRegistrationRequestFilter) filter));
        }
        if (filter instanceof BrokerRegistrationResponseFilter) {
            responseInvokers.put(62, new BrokerRegistrationResponseFilterInvoker((BrokerRegistrationResponseFilter) filter));
        }
        if (filter instanceof ControlledShutdownRequestFilter) {
            requestInvokers.put(7, new ControlledShutdownRequestFilterInvoker((ControlledShutdownRequestFilter) filter));
        }
        if (filter instanceof ControlledShutdownResponseFilter) {
            responseInvokers.put(7, new ControlledShutdownResponseFilterInvoker((ControlledShutdownResponseFilter) filter));
        }
        if (filter instanceof CreateAclsRequestFilter) {
            requestInvokers.put(30, new CreateAclsRequestFilterInvoker((CreateAclsRequestFilter) filter));
        }
        if (filter instanceof CreateAclsResponseFilter) {
            responseInvokers.put(30, new CreateAclsResponseFilterInvoker((CreateAclsResponseFilter) filter));
        }
        if (filter instanceof CreateDelegationTokenRequestFilter) {
            requestInvokers.put(38, new CreateDelegationTokenRequestFilterInvoker((CreateDelegationTokenRequestFilter) filter));
        }
        if (filter instanceof CreateDelegationTokenResponseFilter) {
            responseInvokers.put(38, new CreateDelegationTokenResponseFilterInvoker((CreateDelegationTokenResponseFilter) filter));
        }
        if (filter instanceof CreatePartitionsRequestFilter) {
            requestInvokers.put(37, new CreatePartitionsRequestFilterInvoker((CreatePartitionsRequestFilter) filter));
        }
        if (filter instanceof CreatePartitionsResponseFilter) {
            responseInvokers.put(37, new CreatePartitionsResponseFilterInvoker((CreatePartitionsResponseFilter) filter));
        }
        if (filter instanceof CreateTopicsRequestFilter) {
            requestInvokers.put(19, new CreateTopicsRequestFilterInvoker((CreateTopicsRequestFilter) filter));
        }
        if (filter instanceof CreateTopicsResponseFilter) {
            responseInvokers.put(19, new CreateTopicsResponseFilterInvoker((CreateTopicsResponseFilter) filter));
        }
        if (filter instanceof DeleteAclsRequestFilter) {
            requestInvokers.put(31, new DeleteAclsRequestFilterInvoker((DeleteAclsRequestFilter) filter));
        }
        if (filter instanceof DeleteAclsResponseFilter) {
            responseInvokers.put(31, new DeleteAclsResponseFilterInvoker((DeleteAclsResponseFilter) filter));
        }
        if (filter instanceof DeleteGroupsRequestFilter) {
            requestInvokers.put(42, new DeleteGroupsRequestFilterInvoker((DeleteGroupsRequestFilter) filter));
        }
        if (filter instanceof DeleteGroupsResponseFilter) {
            responseInvokers.put(42, new DeleteGroupsResponseFilterInvoker((DeleteGroupsResponseFilter) filter));
        }
        if (filter instanceof DeleteRecordsRequestFilter) {
            requestInvokers.put(21, new DeleteRecordsRequestFilterInvoker((DeleteRecordsRequestFilter) filter));
        }
        if (filter instanceof DeleteRecordsResponseFilter) {
            responseInvokers.put(21, new DeleteRecordsResponseFilterInvoker((DeleteRecordsResponseFilter) filter));
        }
        if (filter instanceof DeleteTopicsRequestFilter) {
            requestInvokers.put(20, new DeleteTopicsRequestFilterInvoker((DeleteTopicsRequestFilter) filter));
        }
        if (filter instanceof DeleteTopicsResponseFilter) {
            responseInvokers.put(20, new DeleteTopicsResponseFilterInvoker((DeleteTopicsResponseFilter) filter));
        }
        if (filter instanceof DescribeAclsRequestFilter) {
            requestInvokers.put(29, new DescribeAclsRequestFilterInvoker((DescribeAclsRequestFilter) filter));
        }
        if (filter instanceof DescribeAclsResponseFilter) {
            responseInvokers.put(29, new DescribeAclsResponseFilterInvoker((DescribeAclsResponseFilter) filter));
        }
        if (filter instanceof DescribeClientQuotasRequestFilter) {
            requestInvokers.put(48, new DescribeClientQuotasRequestFilterInvoker((DescribeClientQuotasRequestFilter) filter));
        }
        if (filter instanceof DescribeClientQuotasResponseFilter) {
            responseInvokers.put(48, new DescribeClientQuotasResponseFilterInvoker((DescribeClientQuotasResponseFilter) filter));
        }
        if (filter instanceof DescribeClusterRequestFilter) {
            requestInvokers.put(60, new DescribeClusterRequestFilterInvoker((DescribeClusterRequestFilter) filter));
        }
        if (filter instanceof DescribeClusterResponseFilter) {
            responseInvokers.put(60, new DescribeClusterResponseFilterInvoker((DescribeClusterResponseFilter) filter));
        }
        if (filter instanceof DescribeConfigsRequestFilter) {
            requestInvokers.put(32, new DescribeConfigsRequestFilterInvoker((DescribeConfigsRequestFilter) filter));
        }
        if (filter instanceof DescribeConfigsResponseFilter) {
            responseInvokers.put(32, new DescribeConfigsResponseFilterInvoker((DescribeConfigsResponseFilter) filter));
        }
        if (filter instanceof DescribeDelegationTokenRequestFilter) {
            requestInvokers.put(41, new DescribeDelegationTokenRequestFilterInvoker((DescribeDelegationTokenRequestFilter) filter));
        }
        if (filter instanceof DescribeDelegationTokenResponseFilter) {
            responseInvokers.put(41, new DescribeDelegationTokenResponseFilterInvoker((DescribeDelegationTokenResponseFilter) filter));
        }
        if (filter instanceof DescribeGroupsRequestFilter) {
            requestInvokers.put(15, new DescribeGroupsRequestFilterInvoker((DescribeGroupsRequestFilter) filter));
        }
        if (filter instanceof DescribeGroupsResponseFilter) {
            responseInvokers.put(15, new DescribeGroupsResponseFilterInvoker((DescribeGroupsResponseFilter) filter));
        }
        if (filter instanceof DescribeLogDirsRequestFilter) {
            requestInvokers.put(35, new DescribeLogDirsRequestFilterInvoker((DescribeLogDirsRequestFilter) filter));
        }
        if (filter instanceof DescribeLogDirsResponseFilter) {
            responseInvokers.put(35, new DescribeLogDirsResponseFilterInvoker((DescribeLogDirsResponseFilter) filter));
        }
        if (filter instanceof DescribeProducersRequestFilter) {
            requestInvokers.put(61, new DescribeProducersRequestFilterInvoker((DescribeProducersRequestFilter) filter));
        }
        if (filter instanceof DescribeProducersResponseFilter) {
            responseInvokers.put(61, new DescribeProducersResponseFilterInvoker((DescribeProducersResponseFilter) filter));
        }
        if (filter instanceof DescribeQuorumRequestFilter) {
            requestInvokers.put(55, new DescribeQuorumRequestFilterInvoker((DescribeQuorumRequestFilter) filter));
        }
        if (filter instanceof DescribeQuorumResponseFilter) {
            responseInvokers.put(55, new DescribeQuorumResponseFilterInvoker((DescribeQuorumResponseFilter) filter));
        }
        if (filter instanceof DescribeTransactionsRequestFilter) {
            requestInvokers.put(65, new DescribeTransactionsRequestFilterInvoker((DescribeTransactionsRequestFilter) filter));
        }
        if (filter instanceof DescribeTransactionsResponseFilter) {
            responseInvokers.put(65, new DescribeTransactionsResponseFilterInvoker((DescribeTransactionsResponseFilter) filter));
        }
        if (filter instanceof DescribeUserScramCredentialsRequestFilter) {
            requestInvokers.put(50, new DescribeUserScramCredentialsRequestFilterInvoker((DescribeUserScramCredentialsRequestFilter) filter));
        }
        if (filter instanceof DescribeUserScramCredentialsResponseFilter) {
            responseInvokers.put(50, new DescribeUserScramCredentialsResponseFilterInvoker((DescribeUserScramCredentialsResponseFilter) filter));
        }
        if (filter instanceof ElectLeadersRequestFilter) {
            requestInvokers.put(43, new ElectLeadersRequestFilterInvoker((ElectLeadersRequestFilter) filter));
        }
        if (filter instanceof ElectLeadersResponseFilter) {
            responseInvokers.put(43, new ElectLeadersResponseFilterInvoker((ElectLeadersResponseFilter) filter));
        }
        if (filter instanceof EndQuorumEpochRequestFilter) {
            requestInvokers.put(54, new EndQuorumEpochRequestFilterInvoker((EndQuorumEpochRequestFilter) filter));
        }
        if (filter instanceof EndQuorumEpochResponseFilter) {
            responseInvokers.put(54, new EndQuorumEpochResponseFilterInvoker((EndQuorumEpochResponseFilter) filter));
        }
        if (filter instanceof EndTxnRequestFilter) {
            requestInvokers.put(26, new EndTxnRequestFilterInvoker((EndTxnRequestFilter) filter));
        }
        if (filter instanceof EndTxnResponseFilter) {
            responseInvokers.put(26, new EndTxnResponseFilterInvoker((EndTxnResponseFilter) filter));
        }
        if (filter instanceof EnvelopeRequestFilter) {
            requestInvokers.put(58, new EnvelopeRequestFilterInvoker((EnvelopeRequestFilter) filter));
        }
        if (filter instanceof EnvelopeResponseFilter) {
            responseInvokers.put(58, new EnvelopeResponseFilterInvoker((EnvelopeResponseFilter) filter));
        }
        if (filter instanceof ExpireDelegationTokenRequestFilter) {
            requestInvokers.put(40, new ExpireDelegationTokenRequestFilterInvoker((ExpireDelegationTokenRequestFilter) filter));
        }
        if (filter instanceof ExpireDelegationTokenResponseFilter) {
            responseInvokers.put(40, new ExpireDelegationTokenResponseFilterInvoker((ExpireDelegationTokenResponseFilter) filter));
        }
        if (filter instanceof FetchRequestFilter) {
            requestInvokers.put(1, new FetchRequestFilterInvoker((FetchRequestFilter) filter));
        }
        if (filter instanceof FetchResponseFilter) {
            responseInvokers.put(1, new FetchResponseFilterInvoker((FetchResponseFilter) filter));
        }
        if (filter instanceof FetchSnapshotRequestFilter) {
            requestInvokers.put(59, new FetchSnapshotRequestFilterInvoker((FetchSnapshotRequestFilter) filter));
        }
        if (filter instanceof FetchSnapshotResponseFilter) {
            responseInvokers.put(59, new FetchSnapshotResponseFilterInvoker((FetchSnapshotResponseFilter) filter));
        }
        if (filter instanceof FindCoordinatorRequestFilter) {
            requestInvokers.put(10, new FindCoordinatorRequestFilterInvoker((FindCoordinatorRequestFilter) filter));
        }
        if (filter instanceof FindCoordinatorResponseFilter) {
            responseInvokers.put(10, new FindCoordinatorResponseFilterInvoker((FindCoordinatorResponseFilter) filter));
        }
        if (filter instanceof HeartbeatRequestFilter) {
            requestInvokers.put(12, new HeartbeatRequestFilterInvoker((HeartbeatRequestFilter) filter));
        }
        if (filter instanceof HeartbeatResponseFilter) {
            responseInvokers.put(12, new HeartbeatResponseFilterInvoker((HeartbeatResponseFilter) filter));
        }
        if (filter instanceof IncrementalAlterConfigsRequestFilter) {
            requestInvokers.put(44, new IncrementalAlterConfigsRequestFilterInvoker((IncrementalAlterConfigsRequestFilter) filter));
        }
        if (filter instanceof IncrementalAlterConfigsResponseFilter) {
            responseInvokers.put(44, new IncrementalAlterConfigsResponseFilterInvoker((IncrementalAlterConfigsResponseFilter) filter));
        }
        if (filter instanceof InitProducerIdRequestFilter) {
            requestInvokers.put(22, new InitProducerIdRequestFilterInvoker((InitProducerIdRequestFilter) filter));
        }
        if (filter instanceof InitProducerIdResponseFilter) {
            responseInvokers.put(22, new InitProducerIdResponseFilterInvoker((InitProducerIdResponseFilter) filter));
        }
        if (filter instanceof JoinGroupRequestFilter) {
            requestInvokers.put(11, new JoinGroupRequestFilterInvoker((JoinGroupRequestFilter) filter));
        }
        if (filter instanceof JoinGroupResponseFilter) {
            responseInvokers.put(11, new JoinGroupResponseFilterInvoker((JoinGroupResponseFilter) filter));
        }
        if (filter instanceof LeaderAndIsrRequestFilter) {
            requestInvokers.put(4, new LeaderAndIsrRequestFilterInvoker((LeaderAndIsrRequestFilter) filter));
        }
        if (filter instanceof LeaderAndIsrResponseFilter) {
            responseInvokers.put(4, new LeaderAndIsrResponseFilterInvoker((LeaderAndIsrResponseFilter) filter));
        }
        if (filter instanceof LeaveGroupRequestFilter) {
            requestInvokers.put(13, new LeaveGroupRequestFilterInvoker((LeaveGroupRequestFilter) filter));
        }
        if (filter instanceof LeaveGroupResponseFilter) {
            responseInvokers.put(13, new LeaveGroupResponseFilterInvoker((LeaveGroupResponseFilter) filter));
        }
        if (filter instanceof ListGroupsRequestFilter) {
            requestInvokers.put(16, new ListGroupsRequestFilterInvoker((ListGroupsRequestFilter) filter));
        }
        if (filter instanceof ListGroupsResponseFilter) {
            responseInvokers.put(16, new ListGroupsResponseFilterInvoker((ListGroupsResponseFilter) filter));
        }
        if (filter instanceof ListOffsetsRequestFilter) {
            requestInvokers.put(2, new ListOffsetsRequestFilterInvoker((ListOffsetsRequestFilter) filter));
        }
        if (filter instanceof ListOffsetsResponseFilter) {
            responseInvokers.put(2, new ListOffsetsResponseFilterInvoker((ListOffsetsResponseFilter) filter));
        }
        if (filter instanceof ListPartitionReassignmentsRequestFilter) {
            requestInvokers.put(46, new ListPartitionReassignmentsRequestFilterInvoker((ListPartitionReassignmentsRequestFilter) filter));
        }
        if (filter instanceof ListPartitionReassignmentsResponseFilter) {
            responseInvokers.put(46, new ListPartitionReassignmentsResponseFilterInvoker((ListPartitionReassignmentsResponseFilter) filter));
        }
        if (filter instanceof ListTransactionsRequestFilter) {
            requestInvokers.put(66, new ListTransactionsRequestFilterInvoker((ListTransactionsRequestFilter) filter));
        }
        if (filter instanceof ListTransactionsResponseFilter) {
            responseInvokers.put(66, new ListTransactionsResponseFilterInvoker((ListTransactionsResponseFilter) filter));
        }
        if (filter instanceof MetadataRequestFilter) {
            requestInvokers.put(3, new MetadataRequestFilterInvoker((MetadataRequestFilter) filter));
        }
        if (filter instanceof MetadataResponseFilter) {
            responseInvokers.put(3, new MetadataResponseFilterInvoker((MetadataResponseFilter) filter));
        }
        if (filter instanceof OffsetCommitRequestFilter) {
            requestInvokers.put(8, new OffsetCommitRequestFilterInvoker((OffsetCommitRequestFilter) filter));
        }
        if (filter instanceof OffsetCommitResponseFilter) {
            responseInvokers.put(8, new OffsetCommitResponseFilterInvoker((OffsetCommitResponseFilter) filter));
        }
        if (filter instanceof OffsetDeleteRequestFilter) {
            requestInvokers.put(47, new OffsetDeleteRequestFilterInvoker((OffsetDeleteRequestFilter) filter));
        }
        if (filter instanceof OffsetDeleteResponseFilter) {
            responseInvokers.put(47, new OffsetDeleteResponseFilterInvoker((OffsetDeleteResponseFilter) filter));
        }
        if (filter instanceof OffsetFetchRequestFilter) {
            requestInvokers.put(9, new OffsetFetchRequestFilterInvoker((OffsetFetchRequestFilter) filter));
        }
        if (filter instanceof OffsetFetchResponseFilter) {
            responseInvokers.put(9, new OffsetFetchResponseFilterInvoker((OffsetFetchResponseFilter) filter));
        }
        if (filter instanceof OffsetForLeaderEpochRequestFilter) {
            requestInvokers.put(23, new OffsetForLeaderEpochRequestFilterInvoker((OffsetForLeaderEpochRequestFilter) filter));
        }
        if (filter instanceof OffsetForLeaderEpochResponseFilter) {
            responseInvokers.put(23, new OffsetForLeaderEpochResponseFilterInvoker((OffsetForLeaderEpochResponseFilter) filter));
        }
        if (filter instanceof ProduceRequestFilter) {
            requestInvokers.put(0, new ProduceRequestFilterInvoker((ProduceRequestFilter) filter));
        }
        if (filter instanceof ProduceResponseFilter) {
            responseInvokers.put(0, new ProduceResponseFilterInvoker((ProduceResponseFilter) filter));
        }
        if (filter instanceof RenewDelegationTokenRequestFilter) {
            requestInvokers.put(39, new RenewDelegationTokenRequestFilterInvoker((RenewDelegationTokenRequestFilter) filter));
        }
        if (filter instanceof RenewDelegationTokenResponseFilter) {
            responseInvokers.put(39, new RenewDelegationTokenResponseFilterInvoker((RenewDelegationTokenResponseFilter) filter));
        }
        if (filter instanceof SaslAuthenticateRequestFilter) {
            requestInvokers.put(36, new SaslAuthenticateRequestFilterInvoker((SaslAuthenticateRequestFilter) filter));
        }
        if (filter instanceof SaslAuthenticateResponseFilter) {
            responseInvokers.put(36, new SaslAuthenticateResponseFilterInvoker((SaslAuthenticateResponseFilter) filter));
        }
        if (filter instanceof SaslHandshakeRequestFilter) {
            requestInvokers.put(17, new SaslHandshakeRequestFilterInvoker((SaslHandshakeRequestFilter) filter));
        }
        if (filter instanceof SaslHandshakeResponseFilter) {
            responseInvokers.put(17, new SaslHandshakeResponseFilterInvoker((SaslHandshakeResponseFilter) filter));
        }
        if (filter instanceof StopReplicaRequestFilter) {
            requestInvokers.put(5, new StopReplicaRequestFilterInvoker((StopReplicaRequestFilter) filter));
        }
        if (filter instanceof StopReplicaResponseFilter) {
            responseInvokers.put(5, new StopReplicaResponseFilterInvoker((StopReplicaResponseFilter) filter));
        }
        if (filter instanceof SyncGroupRequestFilter) {
            requestInvokers.put(14, new SyncGroupRequestFilterInvoker((SyncGroupRequestFilter) filter));
        }
        if (filter instanceof SyncGroupResponseFilter) {
            responseInvokers.put(14, new SyncGroupResponseFilterInvoker((SyncGroupResponseFilter) filter));
        }
        if (filter instanceof TxnOffsetCommitRequestFilter) {
            requestInvokers.put(28, new TxnOffsetCommitRequestFilterInvoker((TxnOffsetCommitRequestFilter) filter));
        }
        if (filter instanceof TxnOffsetCommitResponseFilter) {
            responseInvokers.put(28, new TxnOffsetCommitResponseFilterInvoker((TxnOffsetCommitResponseFilter) filter));
        }
        if (filter instanceof UnregisterBrokerRequestFilter) {
            requestInvokers.put(64, new UnregisterBrokerRequestFilterInvoker((UnregisterBrokerRequestFilter) filter));
        }
        if (filter instanceof UnregisterBrokerResponseFilter) {
            responseInvokers.put(64, new UnregisterBrokerResponseFilterInvoker((UnregisterBrokerResponseFilter) filter));
        }
        if (filter instanceof UpdateFeaturesRequestFilter) {
            requestInvokers.put(57, new UpdateFeaturesRequestFilterInvoker((UpdateFeaturesRequestFilter) filter));
        }
        if (filter instanceof UpdateFeaturesResponseFilter) {
            responseInvokers.put(57, new UpdateFeaturesResponseFilterInvoker((UpdateFeaturesResponseFilter) filter));
        }
        if (filter instanceof UpdateMetadataRequestFilter) {
            requestInvokers.put(6, new UpdateMetadataRequestFilterInvoker((UpdateMetadataRequestFilter) filter));
        }
        if (filter instanceof UpdateMetadataResponseFilter) {
            responseInvokers.put(6, new UpdateMetadataResponseFilterInvoker((UpdateMetadataResponseFilter) filter));
        }
        if (filter instanceof VoteRequestFilter) {
            requestInvokers.put(52, new VoteRequestFilterInvoker((VoteRequestFilter) filter));
        }
        if (filter instanceof VoteResponseFilter) {
            responseInvokers.put(52, new VoteResponseFilterInvoker((VoteResponseFilter) filter));
        }
        if (filter instanceof WriteTxnMarkersRequestFilter) {
            requestInvokers.put(27, new WriteTxnMarkersRequestFilterInvoker((WriteTxnMarkersRequestFilter) filter));
        }
        if (filter instanceof WriteTxnMarkersResponseFilter) {
            responseInvokers.put(27, new WriteTxnMarkersResponseFilterInvoker((WriteTxnMarkersResponseFilter) filter));
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
     * @return
     */
    @Override
    public CompletionStage<RequestFilterResult> onRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeaderData header,
            ApiMessage body,
            FilterContext filterContext
    ) {
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
     * @return
     */
    @Override
    public CompletionStage<ResponseFilterResult> onResponse(
            ApiKeys apiKey,
            short apiVersion,
            ResponseHeaderData header,
            ApiMessage body,
            FilterContext filterContext
    ) {
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
