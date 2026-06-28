/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
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
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
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
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListConfigResourcesRequestData;
import org.apache.kafka.common.message.ListConfigResourcesResponseData;
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
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
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
import org.apache.kafka.common.requests.ListConfigResourcesResponse;
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

    private static ListConfigResourcesResponse newListConfigResourcesV0ErrorResponse(Errors apiException) {
        return new ListConfigResourcesResponse(new ListConfigResourcesResponseData()
                .setErrorCode(apiException.code())
                .setConfigResources(List.of(new ListConfigResourcesResponseData.ConfigResource().setResourceType((byte) 16))));
    }

    public static AbstractResponse errorResponseForMessage(RequestHeaderData requestHeaders, ApiMessage message, ApiException apiException) {
        final short apiKey = message.apiKey();
        // Our ListConfigResourcesRequestData is deserialized off the wire and so will only have the v0 fields populated.
        // we can't just all errorResponse, which in turn uses a ListConfigResourcesRequest.Builder,
        // because that builder applies some validation that's inappropriate for our purposes.
        if (apiKey == ApiKeys.LIST_CONFIG_RESOURCES.id && requestHeaders.requestApiVersion() == 0) {
            return newListConfigResourcesV0ErrorResponse(Errors.forException(apiException));
        }
        return errorResponse(ApiKeys.forId(apiKey), message, requestHeaders.requestApiVersion()).getErrorResponse(apiException);
    }

    @VisibleForTesting
    static AbstractResponse errorResponse(DecodedRequestFrame<?> frame, Throwable error) {
        ApiMessage reqBody = frame.body();
        short apiVersion = frame.apiVersion();
        final ApiKeys apiKey = frame.apiKey();
        if (frame.apiKey() == ApiKeys.LIST_CONFIG_RESOURCES && frame.apiVersion() == 0) {
            return newListConfigResourcesV0ErrorResponse(Errors.forException(error));
        }
        final AbstractRequest req = errorResponse(apiKey, reqBody, apiVersion);
        return req.getErrorResponse(error);
    }

    /*
     * This monstrosity is needed because there isn't any _nicely_ abstracted code we can borrow from Kafka
     * which creates and response with error codes set appropriately.
     */
    private static AbstractRequest errorResponse(ApiKeys apiKey, ApiMessage reqBody, short apiVersion) {
        return switch (apiKey) {
            case SASL_HANDSHAKE -> new SaslHandshakeRequest((SaslHandshakeRequestData) reqBody, apiVersion);
            case SASL_AUTHENTICATE -> new SaslAuthenticateRequest((SaslAuthenticateRequestData) reqBody, apiVersion);
            case PRODUCE -> new ProduceRequest((ProduceRequestData) reqBody, apiVersion);
            case FETCH -> new FetchRequest((FetchRequestData) reqBody, apiVersion);
            case LIST_OFFSETS -> {
                ListOffsetsRequestData listOffsetsRequestData = (ListOffsetsRequestData) reqBody;
                if (listOffsetsRequestData.replicaId() == ListOffsetsRequest.CONSUMER_REPLICA_ID) {
                    yield ListOffsetsRequest.Builder.forConsumer(true,
                            IsolationLevel.forId(listOffsetsRequestData.isolationLevel()))
                            .setTargetTimes(listOffsetsRequestData.topics())
                            .build(apiVersion);
                }
                else {
                    yield ListOffsetsRequest.Builder.forReplica(apiVersion, listOffsetsRequestData.replicaId())
                            .setTargetTimes(listOffsetsRequestData.topics())
                            .build(apiVersion);
                }
            }
            case METADATA -> new MetadataRequest((MetadataRequestData) reqBody, apiVersion);
            case OFFSET_COMMIT -> new OffsetCommitRequest((OffsetCommitRequestData) reqBody, apiVersion);
            case OFFSET_FETCH -> {
                OffsetFetchRequestData offsetFetchRequestData = (OffsetFetchRequestData) reqBody;
                yield OffsetFetchRequest.Builder.forTopicIdsOrNames(offsetFetchRequestData, false).build(apiVersion);
            }
            case FIND_COORDINATOR -> new FindCoordinatorRequest.Builder((FindCoordinatorRequestData) reqBody)
                    .build(apiVersion);
            case JOIN_GROUP -> new JoinGroupRequest((JoinGroupRequestData) reqBody, apiVersion);
            case HEARTBEAT -> new HeartbeatRequest.Builder((HeartbeatRequestData) reqBody)
                    .build(apiVersion);
            case LEAVE_GROUP -> {
                LeaveGroupRequest.Builder builder = toLeaveGroupBuilder((LeaveGroupRequestData) reqBody);
                yield builder.build(apiVersion);
            }
            case SYNC_GROUP -> new SyncGroupRequest((SyncGroupRequestData) reqBody, apiVersion);
            case DESCRIBE_GROUPS -> new DescribeGroupsRequest.Builder((DescribeGroupsRequestData) reqBody)
                    .build(apiVersion);
            case LIST_GROUPS -> new ListGroupsRequest((ListGroupsRequestData) reqBody, apiVersion);
            case API_VERSIONS -> new ApiVersionsRequest((ApiVersionsRequestData) reqBody, apiVersion);
            case CREATE_TOPICS -> new CreateTopicsRequest((CreateTopicsRequestData) reqBody, apiVersion);
            case DELETE_TOPICS -> new DeleteTopicsRequest.Builder((DeleteTopicsRequestData) reqBody)
                    .build(apiVersion);
            case DELETE_RECORDS -> new DeleteRecordsRequest.Builder((DeleteRecordsRequestData) reqBody)
                    .build(apiVersion);
            case INIT_PRODUCER_ID -> new InitProducerIdRequest.Builder((InitProducerIdRequestData) reqBody)
                    .build(apiVersion);
            case OFFSET_FOR_LEADER_EPOCH -> new OffsetsForLeaderEpochRequest((OffsetForLeaderEpochRequestData) reqBody, apiVersion);
            case ADD_PARTITIONS_TO_TXN -> new AddPartitionsToTxnRequest((AddPartitionsToTxnRequestData) reqBody, apiVersion);
            case ADD_OFFSETS_TO_TXN -> new AddOffsetsToTxnRequest((AddOffsetsToTxnRequestData) reqBody, apiVersion);
            case END_TXN -> new EndTxnRequest.Builder((EndTxnRequestData) reqBody, apiVersion > EndTxnRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2)
                    .build(apiVersion);
            case WRITE_TXN_MARKERS -> new WriteTxnMarkersRequest.Builder((WriteTxnMarkersRequestData) reqBody)
                    .build(apiVersion);
            case TXN_OFFSET_COMMIT -> new TxnOffsetCommitRequest((TxnOffsetCommitRequestData) reqBody, apiVersion);
            case DESCRIBE_ACLS -> {
                DescribeAclsRequestData d = (DescribeAclsRequestData) reqBody;
                yield new DescribeAclsRequest.Builder(new AclBindingFilter(
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
            }
            case CREATE_ACLS -> new CreateAclsRequest.Builder((CreateAclsRequestData) reqBody)
                    .build(apiVersion);
            case DELETE_ACLS -> new DeleteAclsRequest.Builder((DeleteAclsRequestData) reqBody).build(apiVersion);
            case DESCRIBE_CONFIGS -> new DescribeConfigsRequest((DescribeConfigsRequestData) reqBody, apiVersion);
            case ALTER_CONFIGS -> new AlterConfigsRequest((AlterConfigsRequestData) reqBody, apiVersion);
            case ALTER_REPLICA_LOG_DIRS -> new AlterReplicaLogDirsRequest((AlterReplicaLogDirsRequestData) reqBody, apiVersion);
            case DESCRIBE_LOG_DIRS -> new DescribeLogDirsRequest((DescribeLogDirsRequestData) reqBody, apiVersion);
            case CREATE_PARTITIONS -> new CreatePartitionsRequest.Builder((CreatePartitionsRequestData) reqBody)
                    .build(apiVersion);
            case CREATE_DELEGATION_TOKEN -> new CreateDelegationTokenRequest.Builder((CreateDelegationTokenRequestData) reqBody)
                    .build(apiVersion);
            case RENEW_DELEGATION_TOKEN -> new RenewDelegationTokenRequest.Builder((RenewDelegationTokenRequestData) reqBody)
                    .build(apiVersion);
            case EXPIRE_DELEGATION_TOKEN -> new ExpireDelegationTokenRequest.Builder((ExpireDelegationTokenRequestData) reqBody)
                    .build(apiVersion);
            case DESCRIBE_DELEGATION_TOKEN -> {
                DescribeDelegationTokenRequestData tokenRequestData = (DescribeDelegationTokenRequestData) reqBody;
                yield new DescribeDelegationTokenRequest.Builder(
                        tokenRequestData.owners().stream().map(o -> new KafkaPrincipal(o.principalType(), o.principalName())).toList())
                        .build(apiVersion);
            }
            case DELETE_GROUPS -> new DeleteGroupsRequest((DeleteGroupsRequestData) reqBody, apiVersion);
            case ELECT_LEADERS -> {
                ElectLeadersRequestData electLeaders = (ElectLeadersRequestData) reqBody;
                yield new ElectLeadersRequest.Builder(
                        ElectionType.valueOf(electLeaders.electionType()),
                        electLeaders.topicPartitions().stream().flatMap(
                                t -> t.partitions().stream().map(
                                        p -> new TopicPartition(t.topic(), p)))
                                .toList(),
                        electLeaders.timeoutMs())
                        .build(apiVersion);
            }
            case INCREMENTAL_ALTER_CONFIGS -> new IncrementalAlterConfigsRequest((IncrementalAlterConfigsRequestData) reqBody, apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS -> new AlterPartitionReassignmentsRequest.Builder((AlterPartitionReassignmentsRequestData) reqBody)
                    .build(apiVersion);
            case LIST_PARTITION_REASSIGNMENTS -> new ListPartitionReassignmentsRequest.Builder((ListPartitionReassignmentsRequestData) reqBody)
                    .build(apiVersion);
            case OFFSET_DELETE -> new OffsetDeleteRequest((OffsetDeleteRequestData) reqBody, apiVersion);
            case DESCRIBE_CLIENT_QUOTAS -> new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) reqBody, apiVersion);
            case ALTER_CLIENT_QUOTAS -> new AlterClientQuotasRequest((AlterClientQuotasRequestData) reqBody, apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> new DescribeUserScramCredentialsRequest.Builder((DescribeUserScramCredentialsRequestData) reqBody)
                    .build(apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS -> new AlterUserScramCredentialsRequest.Builder((AlterUserScramCredentialsRequestData) reqBody)
                    .build(apiVersion);
            case DESCRIBE_QUORUM -> new DescribeQuorumRequest.Builder((DescribeQuorumRequestData) reqBody).build(apiVersion);
            case ALTER_PARTITION -> new AlterPartitionRequest((AlterPartitionRequestData) reqBody, apiVersion);
            case UPDATE_FEATURES -> new UpdateFeaturesRequest((UpdateFeaturesRequestData) reqBody, apiVersion);
            case DESCRIBE_CLUSTER -> new DescribeClusterRequest((DescribeClusterRequestData) reqBody, apiVersion);
            case DESCRIBE_PRODUCERS -> new DescribeProducersRequest.Builder((DescribeProducersRequestData) reqBody)
                    .build(apiVersion);
            case DESCRIBE_TRANSACTIONS -> new DescribeTransactionsRequest.Builder((DescribeTransactionsRequestData) reqBody)
                    .build(apiVersion);
            case LIST_TRANSACTIONS -> new ListTransactionsRequest.Builder((ListTransactionsRequestData) reqBody)
                    .build(apiVersion);
            case ALLOCATE_PRODUCER_IDS -> new AllocateProducerIdsRequest((AllocateProducerIdsRequestData) reqBody, apiVersion);
            case VOTE -> new VoteRequest.Builder((VoteRequestData) reqBody)
                    .build(apiVersion);
            case BEGIN_QUORUM_EPOCH -> new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) reqBody)
                    .build(apiVersion);
            case END_QUORUM_EPOCH -> new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) reqBody)
                    .build(apiVersion);
            case ENVELOPE -> new EnvelopeRequest((EnvelopeRequestData) reqBody, apiVersion);
            case FETCH_SNAPSHOT -> new FetchSnapshotRequest((FetchSnapshotRequestData) reqBody, apiVersion);
            case BROKER_REGISTRATION -> new BrokerRegistrationRequest((BrokerRegistrationRequestData) reqBody, apiVersion);
            case BROKER_HEARTBEAT -> new BrokerHeartbeatRequest((BrokerHeartbeatRequestData) reqBody, apiVersion);
            case UNREGISTER_BROKER -> new UnregisterBrokerRequest((UnregisterBrokerRequestData) reqBody, apiVersion);
            case CONSUMER_GROUP_HEARTBEAT -> new ConsumerGroupHeartbeatRequest((ConsumerGroupHeartbeatRequestData) reqBody, apiVersion);
            case CONSUMER_GROUP_DESCRIBE -> new ConsumerGroupDescribeRequest((ConsumerGroupDescribeRequestData) reqBody, apiVersion);
            case CONTROLLER_REGISTRATION -> new ControllerRegistrationRequest((ControllerRegistrationRequestData) reqBody, apiVersion);
            case GET_TELEMETRY_SUBSCRIPTIONS -> new GetTelemetrySubscriptionsRequest((GetTelemetrySubscriptionsRequestData) reqBody, apiVersion);
            case PUSH_TELEMETRY -> new PushTelemetryRequest((PushTelemetryRequestData) reqBody, apiVersion);
            case ASSIGN_REPLICAS_TO_DIRS -> new AssignReplicasToDirsRequest((AssignReplicasToDirsRequestData) reqBody, apiVersion);
            case LIST_CONFIG_RESOURCES -> new ListConfigResourcesRequest.Builder((ListConfigResourcesRequestData) reqBody).build(apiVersion);
            case DESCRIBE_TOPIC_PARTITIONS -> new DescribeTopicPartitionsRequest((DescribeTopicPartitionsRequestData) reqBody, apiVersion);
            case ADD_RAFT_VOTER -> new AddRaftVoterRequest((AddRaftVoterRequestData) reqBody, apiVersion);
            case REMOVE_RAFT_VOTER -> new RemoveRaftVoterRequest((RemoveRaftVoterRequestData) reqBody, apiVersion);
            case UPDATE_RAFT_VOTER -> new UpdateRaftVoterRequest((UpdateRaftVoterRequestData) reqBody, apiVersion);
            case SHARE_GROUP_HEARTBEAT -> new ShareGroupHeartbeatRequest((ShareGroupHeartbeatRequestData) reqBody, apiVersion);
            case SHARE_GROUP_DESCRIBE -> new ShareGroupDescribeRequest((ShareGroupDescribeRequestData) reqBody, apiVersion);
            case SHARE_FETCH -> new ShareFetchRequest((ShareFetchRequestData) reqBody, apiVersion);
            case SHARE_ACKNOWLEDGE -> new ShareAcknowledgeRequest((ShareAcknowledgeRequestData) reqBody, apiVersion);
            case INITIALIZE_SHARE_GROUP_STATE -> new InitializeShareGroupStateRequest((InitializeShareGroupStateRequestData) reqBody, apiVersion);
            case READ_SHARE_GROUP_STATE -> new ReadShareGroupStateRequest((ReadShareGroupStateRequestData) reqBody, apiVersion);
            case WRITE_SHARE_GROUP_STATE -> new WriteShareGroupStateRequest((WriteShareGroupStateRequestData) reqBody, apiVersion);
            case DELETE_SHARE_GROUP_STATE -> new DeleteShareGroupStateRequest((DeleteShareGroupStateRequestData) reqBody, apiVersion);
            case READ_SHARE_GROUP_STATE_SUMMARY -> new ReadShareGroupStateSummaryRequest((ReadShareGroupStateSummaryRequestData) reqBody, apiVersion);
            case STREAMS_GROUP_HEARTBEAT -> new StreamsGroupHeartbeatRequest((StreamsGroupHeartbeatRequestData) reqBody, apiVersion);
            case STREAMS_GROUP_DESCRIBE -> new StreamsGroupDescribeRequest((StreamsGroupDescribeRequestData) reqBody, apiVersion);
            case DESCRIBE_SHARE_GROUP_OFFSETS -> new DescribeShareGroupOffsetsRequest((DescribeShareGroupOffsetsRequestData) reqBody, apiVersion);
            case ALTER_SHARE_GROUP_OFFSETS -> new AlterShareGroupOffsetsRequest.Builder((AlterShareGroupOffsetsRequestData) reqBody).build(apiVersion);
            case DELETE_SHARE_GROUP_OFFSETS -> new DeleteShareGroupOffsetsRequest((DeleteShareGroupOffsetsRequestData) reqBody, apiVersion);
            default -> throw new IllegalStateException("Unable to generate error for APIKey: " + apiKey);
        };
    }

    private static LeaveGroupRequest.Builder toLeaveGroupBuilder(LeaveGroupRequestData reqBody) {
        LeaveGroupRequest.Builder builder;
        if (!reqBody.members().isEmpty()) {
            builder = new LeaveGroupRequest.Builder(reqBody.groupId(), reqBody.members());
        }
        else {
            // This should never happen with a legitimate client but as a edge service we should handle malicious ones too
            builder = new LeaveGroupRequest.Builder(reqBody.groupId(), List.of(new LeaveGroupRequestData.MemberIdentity()));
        }
        return builder;
    }
}
