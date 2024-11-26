/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Collectors;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListClientMetricsResourcesRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
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
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListClientMetricsResourcesRequest;
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
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.UpdateRaftVoterRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * In the operation of the proxy there are various exceptions which are "anticipated" but not necessarily handled directly.
 * SSL Handshake errors are an illustrative example, where they are thrown and propagate through the netty channel without being handled.
 * <p>
 * The exception mapper provides a mechanism to register specific exceptions with function to evaluate them and if appropriate generate a message to respond to the
 * client with.
 */
public class KafkaProxyExceptionMapper {

    private KafkaProxyExceptionMapper() {
    }

    public static ApiMessage errorResponseMessage(DecodedRequestFrame<?> frame, Throwable error) {
        return errorResponse(frame, error).data();
    }

    public static AbstractResponse errorResponseForMessage(ApiMessage message, Throwable error) {
        final short apiKey = message.apiKey();
        return errorResponse(ApiKeys.forId(apiKey), message, message.highestSupportedVersion()).getErrorResponse(error);
    }

    @VisibleForTesting
    static AbstractResponse errorResponse(DecodedRequestFrame<?> frame, Throwable error) {
        ApiMessage reqBody = frame.body();
        short apiVersion = frame.apiVersion();
        final ApiKeys apiKey = frame.apiKey();
        final AbstractRequest req = errorResponse(apiKey, reqBody, apiVersion);
        return req.getErrorResponse(error);
    }

    /*
     * This monstrosity is needed because there isn't any _nicely_ abstracted code we can borrow from Kafka
     * which creates and response with error codes set appropriately.
     */
    private static AbstractRequest errorResponse(ApiKeys apiKey, ApiMessage reqBody, short apiVersion) {
        final AbstractRequest req;
        switch (apiKey) {
            case SASL_HANDSHAKE:
                req = new SaslHandshakeRequest((SaslHandshakeRequestData) reqBody, apiVersion);
                break;
            case SASL_AUTHENTICATE:
                req = new SaslAuthenticateRequest((SaslAuthenticateRequestData) reqBody, apiVersion);
                break;
            case PRODUCE:
                req = new ProduceRequest((ProduceRequestData) reqBody, apiVersion);
                break;
            case FETCH:
                req = new FetchRequest((FetchRequestData) reqBody, apiVersion);
                break;
            case LIST_OFFSETS:
                ListOffsetsRequestData listOffsetsRequestData = (ListOffsetsRequestData) reqBody;
                if (listOffsetsRequestData.replicaId() == ListOffsetsRequest.CONSUMER_REPLICA_ID) {
                    req = ListOffsetsRequest.Builder.forConsumer(true,
                            IsolationLevel.forId(listOffsetsRequestData.isolationLevel()))
                            .setTargetTimes(listOffsetsRequestData.topics())
                            .build(apiVersion);
                }
                else {
                    req = ListOffsetsRequest.Builder.forReplica(apiVersion, listOffsetsRequestData.replicaId())
                            .setTargetTimes(listOffsetsRequestData.topics())
                            .build(apiVersion);
                }
                break;
            case METADATA:
                req = new MetadataRequest((MetadataRequestData) reqBody, apiVersion);
                break;
            case OFFSET_COMMIT:
                req = new OffsetCommitRequest((OffsetCommitRequestData) reqBody, apiVersion);
                break;
            case OFFSET_FETCH:
                OffsetFetchRequestData offsetFetchRequestData = (OffsetFetchRequestData) reqBody;
                if (offsetFetchRequestData.groups() != null && !offsetFetchRequestData.groups().isEmpty()) {
                    req = new OffsetFetchRequest.Builder(
                            offsetFetchRequestData.groups().stream().collect(Collectors.toMap(
                                    OffsetFetchRequestData.OffsetFetchRequestGroup::groupId,
                                    x -> x.topics().stream().flatMap(
                                            t -> t.partitionIndexes().stream().map(
                                                    p -> new TopicPartition(t.name(), p)))
                                            .toList())),
                            true, false)
                            .build(apiVersion);
                }
                else if (offsetFetchRequestData.topics() != null && !offsetFetchRequestData.topics().isEmpty()) {
                    req = new OffsetFetchRequest.Builder(
                            offsetFetchRequestData.groupId(),
                            offsetFetchRequestData.requireStable(),
                            offsetFetchRequestData.topics().stream().flatMap(
                                    x -> x.partitionIndexes().stream().map(
                                            p -> new TopicPartition(x.name(), p)))
                                    .toList(),
                            false)
                            .build(apiVersion);
                }
                else {
                    throw new IllegalStateException();
                }
                break;
            case FIND_COORDINATOR:
                req = new FindCoordinatorRequest.Builder((FindCoordinatorRequestData) reqBody)
                        .build(apiVersion);
                break;
            case JOIN_GROUP:
                req = new JoinGroupRequest((JoinGroupRequestData) reqBody, apiVersion);
                break;
            case HEARTBEAT:
                req = new HeartbeatRequest.Builder((HeartbeatRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LEAVE_GROUP:
                LeaveGroupRequestData data = (LeaveGroupRequestData) reqBody;
                req = new LeaveGroupRequest.Builder(data.groupId(), data.members())
                        .build(apiVersion);
                break;
            case SYNC_GROUP:
                req = new SyncGroupRequest((SyncGroupRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_GROUPS:
                req = new DescribeGroupsRequest.Builder((DescribeGroupsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_GROUPS:
                req = new ListGroupsRequest((ListGroupsRequestData) reqBody, apiVersion);
                break;
            case API_VERSIONS:
                req = new ApiVersionsRequest((ApiVersionsRequestData) reqBody, apiVersion);
                break;
            case CREATE_TOPICS:
                req = new CreateTopicsRequest((CreateTopicsRequestData) reqBody, apiVersion);
                break;
            case DELETE_TOPICS:
                req = new DeleteTopicsRequest.Builder((DeleteTopicsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DELETE_RECORDS:
                req = new DeleteRecordsRequest.Builder((DeleteRecordsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case INIT_PRODUCER_ID:
                req = new InitProducerIdRequest.Builder((InitProducerIdRequestData) reqBody)
                        .build(apiVersion);
                break;
            case OFFSET_FOR_LEADER_EPOCH:
                req = new OffsetsForLeaderEpochRequest((OffsetForLeaderEpochRequestData) reqBody, apiVersion);
                break;
            case ADD_PARTITIONS_TO_TXN:
                req = new AddPartitionsToTxnRequest((AddPartitionsToTxnRequestData) reqBody, apiVersion);
                break;
            case ADD_OFFSETS_TO_TXN:
                req = new AddOffsetsToTxnRequest((AddOffsetsToTxnRequestData) reqBody, apiVersion);
                break;
            case END_TXN:
                req = new EndTxnRequest.Builder((EndTxnRequestData) reqBody)
                        .build(apiVersion);
                break;
            case WRITE_TXN_MARKERS:
                req = new WriteTxnMarkersRequest.Builder((WriteTxnMarkersRequestData) reqBody)
                        .build(apiVersion);
                break;
            case TXN_OFFSET_COMMIT:
                req = new TxnOffsetCommitRequest((TxnOffsetCommitRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_ACLS:
                DescribeAclsRequestData d = (DescribeAclsRequestData) reqBody;
                req = new DescribeAclsRequest.Builder(new AclBindingFilter(
                        new ResourcePatternFilter(
                                ResourceType.fromCode(d.resourceTypeFilter()),
                                d.resourceNameFilter(),
                                PatternType.fromCode(d.patternTypeFilter())),
                        new AccessControlEntryFilter(
                                d.principalFilter(),
                                d.hostFilter(),
                                AclOperation.fromCode(d.operation()),
                                AclPermissionType.fromCode(d.permissionType()))))
                        .build(apiVersion);
                break;
            case CREATE_ACLS:
                req = new CreateAclsRequest.Builder((CreateAclsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DELETE_ACLS:
                req = new DeleteAclsRequest.Builder((DeleteAclsRequestData) reqBody).build(apiVersion);
                break;
            case DESCRIBE_CONFIGS:
                req = new DescribeConfigsRequest((DescribeConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_CONFIGS:
                req = new AlterConfigsRequest((AlterConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_REPLICA_LOG_DIRS:
                req = new AlterReplicaLogDirsRequest((AlterReplicaLogDirsRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_LOG_DIRS:
                req = new DescribeLogDirsRequest((DescribeLogDirsRequestData) reqBody, apiVersion);
                break;
            case CREATE_PARTITIONS:
                req = new CreatePartitionsRequest.Builder((CreatePartitionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case CREATE_DELEGATION_TOKEN:
                req = new CreateDelegationTokenRequest.Builder((CreateDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case RENEW_DELEGATION_TOKEN:
                req = new RenewDelegationTokenRequest.Builder((RenewDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case EXPIRE_DELEGATION_TOKEN:
                req = new ExpireDelegationTokenRequest.Builder((ExpireDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_DELEGATION_TOKEN:
                DescribeDelegationTokenRequestData tokenRequestData = (DescribeDelegationTokenRequestData) reqBody;
                req = new DescribeDelegationTokenRequest.Builder(
                        tokenRequestData.owners().stream().map(o -> new KafkaPrincipal(o.principalType(), o.principalName())).toList())
                        .build(apiVersion);
                break;
            case DELETE_GROUPS:
                req = new DeleteGroupsRequest((DeleteGroupsRequestData) reqBody, apiVersion);
                break;
            case ELECT_LEADERS:
                ElectLeadersRequestData electLeaders = (ElectLeadersRequestData) reqBody;
                req = new ElectLeadersRequest.Builder(
                        ElectionType.valueOf(electLeaders.electionType()),
                        electLeaders.topicPartitions().stream().flatMap(
                                t -> t.partitions().stream().map(
                                        p -> new TopicPartition(t.topic(), p)))
                                .toList(),
                        electLeaders.timeoutMs())
                        .build(apiVersion);
                break;
            case INCREMENTAL_ALTER_CONFIGS:
                req = new IncrementalAlterConfigsRequest((IncrementalAlterConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_PARTITION_REASSIGNMENTS:
                req = new AlterPartitionReassignmentsRequest.Builder((AlterPartitionReassignmentsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_PARTITION_REASSIGNMENTS:
                req = new ListPartitionReassignmentsRequest.Builder((ListPartitionReassignmentsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case OFFSET_DELETE:
                req = new OffsetDeleteRequest((OffsetDeleteRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_CLIENT_QUOTAS:
                req = new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) reqBody, apiVersion);
                break;
            case ALTER_CLIENT_QUOTAS:
                req = new AlterClientQuotasRequest((AlterClientQuotasRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                req = new DescribeUserScramCredentialsRequest.Builder((DescribeUserScramCredentialsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ALTER_USER_SCRAM_CREDENTIALS:
                req = new AlterUserScramCredentialsRequest.Builder((AlterUserScramCredentialsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_QUORUM:
                req = new DescribeQuorumRequest.Builder((DescribeQuorumRequestData) reqBody).build(apiVersion);
                break;
            case ALTER_PARTITION:
                req = new AlterPartitionRequest((AlterPartitionRequestData) reqBody, apiVersion);
                break;
            case UPDATE_FEATURES:
                req = new UpdateFeaturesRequest((UpdateFeaturesRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_CLUSTER:
                req = new DescribeClusterRequest((DescribeClusterRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_PRODUCERS:
                req = new DescribeProducersRequest.Builder((DescribeProducersRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_TRANSACTIONS:
                req = new DescribeTransactionsRequest.Builder((DescribeTransactionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_TRANSACTIONS:
                req = new ListTransactionsRequest.Builder((ListTransactionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ALLOCATE_PRODUCER_IDS:
                req = new AllocateProducerIdsRequest((AllocateProducerIdsRequestData) reqBody, apiVersion);
                break;
            case VOTE:
                req = new VoteRequest.Builder((VoteRequestData) reqBody)
                        .build(apiVersion);
                break;
            case BEGIN_QUORUM_EPOCH:
                req = new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) reqBody)
                        .build(apiVersion);
                break;
            case END_QUORUM_EPOCH:
                req = new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ENVELOPE:
                req = new EnvelopeRequest((EnvelopeRequestData) reqBody, apiVersion);
                break;
            case FETCH_SNAPSHOT:
                req = new FetchSnapshotRequest((FetchSnapshotRequestData) reqBody, apiVersion);
                break;
            case LEADER_AND_ISR:
                LeaderAndIsrRequestData lisr = (LeaderAndIsrRequestData) reqBody;
                req = new LeaderAndIsrRequest.Builder(apiVersion, lisr.controllerId(),
                        lisr.controllerEpoch(), lisr.brokerEpoch(),
                        lisr.ungroupedPartitionStates(),
                        lisr.topicStates().stream().collect(Collectors.toMap(
                                LeaderAndIsrRequestData.LeaderAndIsrTopicState::topicName,
                                LeaderAndIsrRequestData.LeaderAndIsrTopicState::topicId)),
                        lisr.liveLeaders().stream().map(
                                x -> new Node(
                                        x.brokerId(),
                                        x.hostName(),
                                        x.port()))
                                .toList())
                        .build(apiVersion);
                break;
            case STOP_REPLICA:
                StopReplicaRequestData stopReplica = (StopReplicaRequestData) reqBody;
                req = new StopReplicaRequest.Builder(apiVersion,
                        stopReplica.controllerId(),
                        stopReplica.controllerEpoch(),
                        stopReplica.brokerEpoch(),
                        stopReplica.deletePartitions(),
                        stopReplica.topicStates())
                        .build(apiVersion);
                break;
            case UPDATE_METADATA:
                req = new UpdateMetadataRequest((UpdateMetadataRequestData) reqBody, apiVersion);
                break;
            case CONTROLLED_SHUTDOWN:
                req = new ControlledShutdownRequest.Builder((ControlledShutdownRequestData) reqBody, apiVersion)
                        .build(apiVersion);
                break;
            case BROKER_REGISTRATION:
                req = new BrokerRegistrationRequest((BrokerRegistrationRequestData) reqBody, apiVersion);
                break;
            case BROKER_HEARTBEAT:
                req = new BrokerHeartbeatRequest((BrokerHeartbeatRequestData) reqBody, apiVersion);
                break;
            case UNREGISTER_BROKER:
                req = new UnregisterBrokerRequest((UnregisterBrokerRequestData) reqBody, apiVersion);
                break;
            case CONSUMER_GROUP_HEARTBEAT:
                req = new ConsumerGroupHeartbeatRequest((ConsumerGroupHeartbeatRequestData) reqBody, apiVersion);
                break;
            case CONSUMER_GROUP_DESCRIBE:
                req = new ConsumerGroupDescribeRequest((ConsumerGroupDescribeRequestData) reqBody, apiVersion);
                break;
            case CONTROLLER_REGISTRATION:
                req = new ControllerRegistrationRequest((ControllerRegistrationRequestData) reqBody, apiVersion);
                break;
            case GET_TELEMETRY_SUBSCRIPTIONS:
                req = new GetTelemetrySubscriptionsRequest((GetTelemetrySubscriptionsRequestData) reqBody, apiVersion);
                break;
            case PUSH_TELEMETRY:
                req = new PushTelemetryRequest((PushTelemetryRequestData) reqBody, apiVersion);
                break;
            case ASSIGN_REPLICAS_TO_DIRS:
                req = new AssignReplicasToDirsRequest((AssignReplicasToDirsRequestData) reqBody, apiVersion);
                break;
            case LIST_CLIENT_METRICS_RESOURCES:
                req = new ListClientMetricsResourcesRequest.Builder((ListClientMetricsResourcesRequestData) reqBody).build(apiVersion);
                break;
            case DESCRIBE_TOPIC_PARTITIONS:
                req = new DescribeTopicPartitionsRequest((DescribeTopicPartitionsRequestData) reqBody, apiVersion);
                break;
            case ADD_RAFT_VOTER:
                req = new AddRaftVoterRequest((AddRaftVoterRequestData) reqBody, apiVersion);
                break;
            case REMOVE_RAFT_VOTER:
                req = new RemoveRaftVoterRequest((RemoveRaftVoterRequestData) reqBody, apiVersion);
                break;
            case UPDATE_RAFT_VOTER:
                req = new UpdateRaftVoterRequest((UpdateRaftVoterRequestData) reqBody, apiVersion);
                break;
            case SHARE_GROUP_HEARTBEAT:
                req = new ShareGroupHeartbeatRequest((ShareGroupHeartbeatRequestData) reqBody, apiVersion);
                break;
            case SHARE_GROUP_DESCRIBE:
                req = new ShareGroupDescribeRequest((ShareGroupDescribeRequestData) reqBody, apiVersion);

                break;
            case SHARE_FETCH:
                req = new ShareFetchRequest((ShareFetchRequestData) reqBody, apiVersion);
                break;
            case SHARE_ACKNOWLEDGE:
                req = new ShareAcknowledgeRequest((ShareAcknowledgeRequestData) reqBody, apiVersion);
                break;
            case INITIALIZE_SHARE_GROUP_STATE:
                req = new InitializeShareGroupStateRequest((InitializeShareGroupStateRequestData) reqBody, apiVersion);
                break;
            case READ_SHARE_GROUP_STATE:
                req = new ReadShareGroupStateRequest((ReadShareGroupStateRequestData) reqBody, apiVersion);
                break;
            case WRITE_SHARE_GROUP_STATE:
                req = new WriteShareGroupStateRequest((WriteShareGroupStateRequestData) reqBody, apiVersion);
                break;
            case DELETE_SHARE_GROUP_STATE:
                req = new DeleteShareGroupStateRequest((DeleteShareGroupStateRequestData) reqBody, apiVersion);
                break;
            case READ_SHARE_GROUP_STATE_SUMMARY:
                req = new ReadShareGroupStateSummaryRequest((ReadShareGroupStateSummaryRequestData) reqBody, apiVersion);
                break;
            default:
                throw new IllegalStateException("Unable to generate error for APIKey: " + apiKey);
        }
        return req;
    }
}
