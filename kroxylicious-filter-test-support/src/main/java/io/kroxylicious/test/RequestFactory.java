/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;
import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

public class RequestFactory {

    private static final short ACKS_ALL = (short) -1;
    // The special cases generally report errors on a per-entry basis rather than globally and thus need to build requests by hand
    // Hopefully they go away one day as we have a sample generator for each type.
    @VisibleForTesting
    protected static final EnumSet<ApiKeys> SPECIAL_CASES = EnumSet.of(ApiKeys.ELECT_LEADERS, ApiKeys.ADD_PARTITIONS_TO_TXN,
            ApiKeys.WRITE_TXN_MARKERS, ApiKeys.TXN_OFFSET_COMMIT, ApiKeys.DESCRIBE_CONFIGS, ApiKeys.ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS,
            ApiKeys.ALTER_REPLICA_LOG_DIRS, ApiKeys.CREATE_PARTITIONS, ApiKeys.ALTER_CLIENT_QUOTAS, ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, ApiKeys.DESCRIBE_PRODUCERS, ApiKeys.DESCRIBE_TRANSACTIONS, ApiKeys.DESCRIBE_TOPIC_PARTITIONS);

    /** The following API keys are no longer used by Kafka. They were removed by Kafka 4.0. */
    @VisibleForTesting
    protected static final EnumSet<ApiKeys> REMOVED_API_KEYS = EnumSet.of(ApiKeys.CONTROLLED_SHUTDOWN, ApiKeys.LEADER_AND_ISR, ApiKeys.STOP_REPLICA,
            ApiKeys.UPDATE_METADATA);

    @VisibleForTesting
    protected static final Map<ApiKeys, Consumer<ApiMessage>> messagePopulators = new EnumMap<>(ApiKeys.class);

    static {
        messagePopulators.put(ApiKeys.PRODUCE, RequestFactory::populateProduceRequest);
        messagePopulators.put(ApiKeys.LIST_OFFSETS, RequestFactory::populateListOffsetsRequest);
        messagePopulators.put(ApiKeys.OFFSET_FETCH, RequestFactory::populateOffsetFetchRequest);
        messagePopulators.put(ApiKeys.METADATA, RequestFactory::populateMetadataRequest);
        messagePopulators.put(ApiKeys.LEAVE_GROUP, RequestFactory::populateLeaveGroupRequest);
        messagePopulators.put(ApiKeys.DESCRIBE_GROUPS, RequestFactory::populateDescribeGroupsRequest);
        messagePopulators.put(ApiKeys.CONSUMER_GROUP_DESCRIBE, RequestFactory::populateConsumeGroupDescribeRequest);
        messagePopulators.put(ApiKeys.DELETE_GROUPS, RequestFactory::populateDeleteGroupRequest);
        messagePopulators.put(ApiKeys.OFFSET_COMMIT, RequestFactory::populateOffsetCommitRequest);
        messagePopulators.put(ApiKeys.CREATE_TOPICS, RequestFactory::populateCreateTopicsRequest);
        messagePopulators.put(ApiKeys.DELETE_TOPICS, RequestFactory::populateDeleteTopicsRequest);
        messagePopulators.put(ApiKeys.DELETE_RECORDS, RequestFactory::populateDeleteRecordsRequest);
        messagePopulators.put(ApiKeys.INIT_PRODUCER_ID, RequestFactory::populateInitProducerIdRequest);
        messagePopulators.put(ApiKeys.CREATE_ACLS, RequestFactory::populateCreateAclsRequest);
        messagePopulators.put(ApiKeys.DESCRIBE_ACLS, RequestFactory::populateDescribeAclsRequest);
        messagePopulators.put(ApiKeys.DELETE_ACLS, RequestFactory::populateDeleteAclsRequest);
        messagePopulators.put(ApiKeys.OFFSET_FOR_LEADER_EPOCH, RequestFactory::populateOffsetForLeaderEpochRequest);
        messagePopulators.put(ApiKeys.SHARE_GROUP_DESCRIBE, RequestFactory::populateShareGroupDescribeRequest);
        messagePopulators.put(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, RequestFactory::populateInitializeShareGroupStateRequest);
        messagePopulators.put(ApiKeys.READ_SHARE_GROUP_STATE, RequestFactory::populateReadShareGroupStateRequest);
        messagePopulators.put(ApiKeys.WRITE_SHARE_GROUP_STATE, RequestFactory::populateWriteShareGroupStateRequest);
        messagePopulators.put(ApiKeys.DELETE_SHARE_GROUP_STATE, RequestFactory::populateDeleteShareGroupStateRequest);
        messagePopulators.put(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY, RequestFactory::populateReadShareGroupStateSummaryRequest);
    }

    private RequestFactory() {
    }

    public static Stream<ApiMessageVersion> apiMessageFor(Function<ApiKeys, Short> versionFunction) {
        return apiMessageFor(versionFunction, supportedApiKeys());
    }

    @NonNull
    public static Set<ApiKeys> supportedApiKeys() {
        var excluded = RequestFactory.SPECIAL_CASES;
        excluded.addAll(REMOVED_API_KEYS);
        return EnumSet.complementOf(excluded);
    }

    public static Stream<ApiMessageVersion> apiMessageFor(Function<ApiKeys, Short> versionFunction, ApiKeys... apiKeys) {
        final EnumSet<ApiKeys> requestedApiKeys = EnumSet.copyOf(Arrays.asList(apiKeys));

        if (SPECIAL_CASES.stream().anyMatch(requestedApiKeys::contains)) {
            throw new IllegalArgumentException("One or more of " + Arrays.toString(apiKeys) + " are not supported.");
        }
        return apiMessageFor(versionFunction, requestedApiKeys);
    }

    public static Stream<ApiMessageVersion> apiMessageFor(Function<ApiKeys, Short> versionFunction, Set<ApiKeys> apiKeys) {
        return Stream.of(apiKeys)
                .flatMap(Collection::stream)
                .filter(Predicate.not(x -> x.messageType.requestSchemas().length == 0 && x.messageType.responseSchemas().length == 0)) // TOOD is there a better way?
                .map(apiKey -> {
                    final ApiMessage apiMessage = apiMessageForApiKey(apiKey);
                    final Short apiVersion = versionFunction.apply(apiKey);
                    return new ApiMessageVersion(apiMessage, apiVersion);
                });
    }

    public record ApiMessageVersion(ApiMessage apiMessage, short apiVersion) {
        public ApiKeys getApiKey() {
            return ApiKeys.forId(apiMessage().apiKey());
        }
    }

    private static @NonNull ApiMessage apiMessageForApiKey(ApiKeys apiKey) {
        final ApiMessage apiMessage = apiKey.messageType.newRequest();
        messagePopulators.getOrDefault(apiKey, message -> {
        }).accept(apiMessage);
        return apiMessage;
    }

    private static void populateProduceRequest(ApiMessage apiMessage) {
        final ProduceRequestData produceRequestData = (ProduceRequestData) apiMessage;
        produceRequestData.setAcks(ACKS_ALL);
        final ProduceRequestData.TopicProduceDataCollection v = new ProduceRequestData.TopicProduceDataCollection(1);
        final ProduceRequestData.TopicProduceData topicProduceData = new ProduceRequestData.TopicProduceData();
        final ProduceRequestData.PartitionProduceData produceData = new ProduceRequestData.PartitionProduceData();
        produceData.setRecords(RecordTestUtils.memoryRecords(List.of(RecordTestUtils.record(MobyNamesGenerator.getRandomName()))));
        topicProduceData.setPartitionData(List.of(produceData));
        v.add(topicProduceData);
        produceRequestData.setTopicData(v);
    }

    private static void populateListOffsetsRequest(ApiMessage apiMessage) {
        final ListOffsetsRequestData listOffsetsRequestData = (ListOffsetsRequestData) apiMessage;
        final ListOffsetsRequestData.ListOffsetsPartition p1 = new ListOffsetsRequestData.ListOffsetsPartition();
        p1.setPartitionIndex(0);
        p1.setCurrentLeaderEpoch(1);
        final ListOffsetsRequestData.ListOffsetsTopic listOffsetsTopic = new ListOffsetsRequestData.ListOffsetsTopic();
        listOffsetsTopic.setName(MobyNamesGenerator.getRandomName());
        listOffsetsTopic.setPartitions(List.of(p1));
        listOffsetsRequestData.setReplicaId(-1);
        listOffsetsRequestData.setTopics(List.of(listOffsetsTopic));
    }

    private static void populateOffsetFetchRequest(ApiMessage apiMessage) {
        final OffsetFetchRequestData offsetFetchRequestData = (OffsetFetchRequestData) apiMessage;
        final OffsetFetchRequestData.OffsetFetchRequestTopic t1 = new OffsetFetchRequestData.OffsetFetchRequestTopic();
        t1.setName(MobyNamesGenerator.getRandomName());
        t1.setPartitionIndexes(List.of(0, 1));
        offsetFetchRequestData.setGroupId(MobyNamesGenerator.getRandomName());
        offsetFetchRequestData.setTopics(List.of(t1));
    }

    private static void populateMetadataRequest(ApiMessage apiMessage) {
        final MetadataRequestData metadataRequestData = (MetadataRequestData) apiMessage;
        final MetadataRequestData.MetadataRequestTopic t1 = new MetadataRequestData.MetadataRequestTopic();
        t1.setName(MobyNamesGenerator.getRandomName());
        t1.setTopicId(Uuid.randomUuid());
        metadataRequestData.setTopics(List.of(t1));
    }

    private static void populateLeaveGroupRequest(ApiMessage apiMessage) {
        final LeaveGroupRequestData leaveGroupRequestData = (LeaveGroupRequestData) apiMessage;
        final LeaveGroupRequestData.MemberIdentity memberIdentity = new LeaveGroupRequestData.MemberIdentity();
        memberIdentity.setMemberId(MobyNamesGenerator.getRandomName());
        leaveGroupRequestData.setMembers(List.of(memberIdentity));
    }

    private static void populateDescribeGroupsRequest(ApiMessage apiMessage) {
        final DescribeGroupsRequestData describeGroupsRequestData = (DescribeGroupsRequestData) apiMessage;
        describeGroupsRequestData.setGroups(List.of(MobyNamesGenerator.getRandomName(), MobyNamesGenerator.getRandomName()));
    }

    private static void populateConsumeGroupDescribeRequest(ApiMessage apiMessage) {
        final ConsumerGroupDescribeRequestData consumerGroupDescribeRequestData = (ConsumerGroupDescribeRequestData) apiMessage;
        consumerGroupDescribeRequestData.setGroupIds(List.of(MobyNamesGenerator.getRandomName(), MobyNamesGenerator.getRandomName()));
    }

    private static void populateDeleteGroupRequest(ApiMessage apiMessage) {
        final DeleteGroupsRequestData deleteGroupsRequestData = (DeleteGroupsRequestData) apiMessage;
        deleteGroupsRequestData.setGroupsNames(List.of(MobyNamesGenerator.getRandomName(), MobyNamesGenerator.getRandomName()));
    }

    private static void populateOffsetCommitRequest(ApiMessage apiMessage) {
        final OffsetCommitRequestData offsetCommitRequestData = (OffsetCommitRequestData) apiMessage;
        offsetCommitRequestData.setGroupId(MobyNamesGenerator.getRandomName());
        offsetCommitRequestData.setMemberId(MobyNamesGenerator.getRandomName());
        final OffsetCommitRequestData.OffsetCommitRequestTopic t1 = new OffsetCommitRequestData.OffsetCommitRequestTopic();
        t1.setName(MobyNamesGenerator.getRandomName());
        final OffsetCommitRequestData.OffsetCommitRequestPartition p1 = new OffsetCommitRequestData.OffsetCommitRequestPartition();
        p1.setCommittedOffset(23456L);
        p1.setPartitionIndex(0);
        t1.setPartitions(List.of(p1));
        offsetCommitRequestData.setTopics(List.of(t1));
    }

    private static void populateCreateTopicsRequest(ApiMessage apiMessage) {
        final CreateTopicsRequestData createTopicsRequestData = (CreateTopicsRequestData) apiMessage;
        final CreateTopicsRequestData.CreatableTopicCollection creatableTopicCollection = new CreateTopicsRequestData.CreatableTopicCollection();
        final CreateTopicsRequestData.CreatableTopic t1 = new CreateTopicsRequestData.CreatableTopic();
        t1.setName(MobyNamesGenerator.getRandomName());
        t1.setNumPartitions(10);
        creatableTopicCollection.add(t1);
        createTopicsRequestData.setTopics(creatableTopicCollection);
    }

    private static void populateDeleteTopicsRequest(ApiMessage apiMessage) {
        final DeleteTopicsRequestData deleteTopicsRequestData = (DeleteTopicsRequestData) apiMessage;
        final DeleteTopicsRequestData.DeleteTopicState t1 = new DeleteTopicsRequestData.DeleteTopicState();
        t1.setTopicId(Uuid.randomUuid());
        t1.setName(MobyNamesGenerator.getRandomName());
        deleteTopicsRequestData.setTopics(List.of(t1));
        deleteTopicsRequestData.setTopicNames(List.of(t1.name()));
    }

    private static void populateDeleteRecordsRequest(ApiMessage apiMessage) {
        final DeleteRecordsRequestData deleteRecordsRequestData = (DeleteRecordsRequestData) apiMessage;
        final DeleteRecordsRequestData.DeleteRecordsTopic t1 = new DeleteRecordsRequestData.DeleteRecordsTopic();
        t1.setName(MobyNamesGenerator.getRandomName());
        final DeleteRecordsRequestData.DeleteRecordsPartition p0 = new DeleteRecordsRequestData.DeleteRecordsPartition();
        p0.setPartitionIndex(10);
        p0.setOffset(876543L);
        t1.setPartitions(List.of(p0));
        deleteRecordsRequestData.setTopics(List.of(t1));
    }

    private static void populateInitProducerIdRequest(ApiMessage apiMessage) {
        final InitProducerIdRequestData initProducerIdRequestData = (InitProducerIdRequestData) apiMessage;
        initProducerIdRequestData.setProducerId(234567L);
        initProducerIdRequestData.setTransactionTimeoutMs(1_000);
        initProducerIdRequestData.setTransactionalId(MobyNamesGenerator.getRandomName());
    }

    private static void populateCreateAclsRequest(ApiMessage apiMessage) {
        final CreateAclsRequestData createAclsRequestData = (CreateAclsRequestData) apiMessage;
        final CreateAclsRequestData.AclCreation aclCreation = new CreateAclsRequestData.AclCreation();
        aclCreation.setPrincipal(MobyNamesGenerator.getRandomName());
        aclCreation.setResourcePatternType(PatternType.LITERAL.code());
        aclCreation.setOperation(AclOperation.ANY.code());
        aclCreation.setPermissionType(AclPermissionType.ANY.code());
        aclCreation.setResourceType(ResourceType.ANY.code());
        createAclsRequestData.setCreations(List.of(aclCreation));
    }

    private static void populateDescribeAclsRequest(ApiMessage apiMessage) {
        final DescribeAclsRequestData describeAclsRequestData = (DescribeAclsRequestData) apiMessage;
        describeAclsRequestData.setPatternTypeFilter(PatternType.ANY.code());
        describeAclsRequestData.setOperation(AclOperation.ANY.code());
        describeAclsRequestData.setPermissionType(AclPermissionType.ANY.code());
        describeAclsRequestData.setResourceNameFilter(MobyNamesGenerator.getRandomName());
        describeAclsRequestData.setResourceTypeFilter(ResourceType.ANY.code());
    }

    private static void populateDeleteAclsRequest(ApiMessage apiMessage) {
        final DeleteAclsRequestData deleteAclsRequestData = (DeleteAclsRequestData) apiMessage;
        final DeleteAclsRequestData.DeleteAclsFilter daf = new DeleteAclsRequestData.DeleteAclsFilter();
        daf.setPatternTypeFilter(PatternType.ANY.code());
        daf.setOperation(AclOperation.ANY.code());
        daf.setPermissionType(AclPermissionType.ANY.code());
        daf.setResourceNameFilter(MobyNamesGenerator.getRandomName());
        daf.setResourceTypeFilter(ResourceType.ANY.code());
        deleteAclsRequestData.setFilters(List.of(daf));
    }

    private static void populateOffsetForLeaderEpochRequest(ApiMessage apiMessage) {
        final OffsetForLeaderEpochRequestData offsetForLeaderEpochRequestData = (OffsetForLeaderEpochRequestData) apiMessage;
        final OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection topicCollection = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection();
        final OffsetForLeaderEpochRequestData.OffsetForLeaderTopic offsetForLeaderTopic = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic();
        offsetForLeaderTopic.setTopic(MobyNamesGenerator.getRandomName());
        final OffsetForLeaderEpochRequestData.OffsetForLeaderPartition p0 = new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition();
        p0.setPartition(0);
        offsetForLeaderTopic.setPartitions(List.of(p0));
        topicCollection.add(offsetForLeaderTopic);
        offsetForLeaderEpochRequestData.setTopics(topicCollection);
    }

    private static void populateShareGroupDescribeRequest(ApiMessage apiMessage) {
        final ShareGroupDescribeRequestData shareGroupDescribeRequestData = (ShareGroupDescribeRequestData) apiMessage;
        shareGroupDescribeRequestData.setGroupIds(List.of(MobyNamesGenerator.getRandomName()));
    }

    private static void populateInitializeShareGroupStateRequest(ApiMessage apiMessage) {
        final InitializeShareGroupStateRequestData shareGroupDescribeRequestData = (InitializeShareGroupStateRequestData) apiMessage;
        final InitializeShareGroupStateRequestData.InitializeStateData initializeStateData = new InitializeShareGroupStateRequestData.InitializeStateData();
        initializeStateData.setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()));
        shareGroupDescribeRequestData.setTopics(List.of(initializeStateData));
    }

    private static void populateReadShareGroupStateRequest(ApiMessage apiMessage) {
        final ReadShareGroupStateRequestData readShareGroupStateRequestData = (ReadShareGroupStateRequestData) apiMessage;
        final ReadShareGroupStateRequestData.ReadStateData readStateData = new ReadShareGroupStateRequestData.ReadStateData();
        readStateData.setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()));
        readShareGroupStateRequestData.setTopics(List.of(readStateData));
    }

    private static void populateWriteShareGroupStateRequest(ApiMessage apiMessage) {
        final WriteShareGroupStateRequestData writeShareGroupStateRequestData = (WriteShareGroupStateRequestData) apiMessage;
        final WriteShareGroupStateRequestData.WriteStateData writeStateData = new WriteShareGroupStateRequestData.WriteStateData();
        writeStateData.setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()));
        writeShareGroupStateRequestData.setTopics(List.of(writeStateData));
    }

    private static void populateDeleteShareGroupStateRequest(ApiMessage apiMessage) {
        final DeleteShareGroupStateRequestData deleteShareGroupStateRequestData = (DeleteShareGroupStateRequestData) apiMessage;
        final DeleteShareGroupStateRequestData.DeleteStateData deleteStateData = new DeleteShareGroupStateRequestData.DeleteStateData();
        deleteStateData.setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()));
        deleteShareGroupStateRequestData.setTopics(List.of(deleteStateData));
    }

    private static void populateReadShareGroupStateSummaryRequest(ApiMessage apiMessage) {
        final ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = (ReadShareGroupStateSummaryRequestData) apiMessage;
        final ReadShareGroupStateSummaryRequestData.ReadStateSummaryData readStateData = new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData();
        readStateData.setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()));
        readShareGroupStateSummaryRequestData.setTopics(List.of(readStateData));
    }
}
