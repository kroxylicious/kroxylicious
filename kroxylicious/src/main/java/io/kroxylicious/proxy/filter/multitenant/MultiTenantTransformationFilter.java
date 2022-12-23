/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.multitenant;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsResponseFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ListOffsetsRequestFilter;
import io.kroxylicious.proxy.filter.ListOffsetsResponseFilter;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.OffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.OffsetFetchRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchResponseFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochRequestFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.internal.filter.FilterConfig;

/**
 * An filter for modifying the key/value/header/topic of {@link ApiKeys#FETCH} responses.
 */
public class MultiTenantTransformationFilter
        implements CreateTopicsRequestFilter, CreateTopicsResponseFilter,
        DeleteTopicsRequestFilter, DeleteTopicsResponseFilter,
        MetadataRequestFilter, MetadataResponseFilter,
        ProduceRequestFilter, ProduceResponseFilter,
        ListOffsetsRequestFilter, ListOffsetsResponseFilter,
        FetchRequestFilter, FetchResponseFilter,
        OffsetFetchRequestFilter, OffsetFetchResponseFilter,
        OffsetCommitRequestFilter, OffsetCommitResponseFilter,
        OffsetForLeaderEpochRequestFilter, OffsetForLeaderEpochResponseFilter {

    public static class MultiTenantTransformationFilterConfig extends FilterConfig {

        private final String tenantPrefix;

        public MultiTenantTransformationFilterConfig(String tenantPrefix) {
            this.tenantPrefix = tenantPrefix;
        }

        public String tenantPrefix() {
            return tenantPrefix;
        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantTransformationFilter.class);

    @Override
    public void onCreateTopicsRequest(CreateTopicsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onCreateTopicsResponse(CreateTopicsResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onDeleteTopicsRequest(DeleteTopicsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, topic.topicId() != null));
        context.forwardRequest(request);
    }

    @Override
    public void onDeleteTopicsResponse(DeleteTopicsResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onMetadataRequest(MetadataRequestData request, KrpcFilterContext context) {
        if (request.topics() != null) {
            request.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false));
        }
        context.forwardRequest(request);

    }

    @Override
    public void onMetadataResponse(MetadataResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        context.forwardResponse(response);

    }

    @Override
    public void onProduceRequest(ProduceRequestData request, KrpcFilterContext context) {
        request.topicData().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false));
        context.forwardRequest(request);

    }

    @Override
    public void onProduceResponse(ProduceResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        context.forwardResponse(response);

    }

    @Override
    public void onListOffsetsRequest(ListOffsetsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onListOffsetsResponse(ListOffsetsResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetFetchRequest(OffsetFetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false));
        request.groups().forEach(requestGroup -> requestGroup.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false)));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetFetchResponse(OffsetFetchResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        response.groups().forEach(responseGroup -> responseGroup.topics().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false)));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetForLeaderEpochRequest(OffsetForLeaderEpochRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::topic, topic::setTopic, false));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetForLeaderEpochResponse(OffsetForLeaderEpochResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(topic::topic, topic::setTopic, false));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetCommitRequest(OffsetCommitRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetCommitResponse(OffsetCommitResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(topic::name, topic::setName, false));
        context.forwardResponse(response);

    }

    @Override
    public void onFetchRequest(FetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(topic::topic, topic::setTopic, topic.topicId() != null));
        context.forwardRequest(request);

    }

    @Override
    public void onFetchResponse(FetchResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(topic::topic, topic::setTopic, topic.topicId() != null));
        context.forwardResponse(response);

    }

    private void applyTenantPrefix(Supplier<String> getter, Consumer<String> setter, boolean ignoreEmpty) {
        String clientSideName = getter.get();
        if (ignoreEmpty && (clientSideName == null || clientSideName.isEmpty())) {
            return;
        }

        setter.accept(String.format("%s-%s", tenantPrefix, clientSideName));
    }

    private void removeTenantPrefix(Supplier<String> getter, Consumer<String> setter, boolean ignoreEmpty) {
        String brokerSideName = getter.get();
        if (ignoreEmpty && (brokerSideName == null || brokerSideName.isEmpty())) {
            return;
        }

        setter.accept(brokerSideName.substring(tenantPrefix.length() + 1));
    }

    /**
     * Transformation to be applied to record value.
     */
    private final String tenantPrefix; // Make me a class that does the mapping.

    // TODO: add transformation support for key/header/topic

    public MultiTenantTransformationFilter(MultiTenantTransformationFilterConfig config) {

        this.tenantPrefix = config.tenantPrefix;
    }

    // @Override
    // public void onFetchResponse(FetchResponseData fetchResponse, KrpcFilterContext context) {
    // List<MetadataRequestData.MetadataRequestTopic> requestTopics = fetchResponse.responses().stream()
    // .filter(t -> t.topic().isEmpty())
    // .map(fetchableTopicResponse -> {
    // Uuid uuid = fetchableTopicResponse.topicId();
    // return new MetadataRequestData.MetadataRequestTopic().setName(null).setTopicId(uuid);
    // })
    // .distinct()
    // .collect(Collectors.toList());
    // if (!requestTopics.isEmpty()) {
    // LOGGER.debug("Fetch response contains {} unknown topic ids, lookup via Metadata request: {}", requestTopics.size(), requestTopics);
    // // TODO Can't necessarily use HIGHEST_SUPPORTED_VERSION, must use highest supported version
    // context.<MetadataResponseData> sendRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
    // new MetadataRequestData()
    // .setTopics(requestTopics))
    // .map(metadataResponse -> {
    // Map<Uuid, String> uidToName = metadataResponse.topics().stream().collect(Collectors.toMap(ti -> ti.topicId(), ti -> ti.name()));
    // LOGGER.debug("Metadata response yields {}, updating original Fetch response", uidToName);
    // for (var fetchableTopicResponse : fetchResponse.responses()) {
    // fetchableTopicResponse.setTopic(uidToName.get(fetchableTopicResponse.topicId()));
    // }
    // applyTransformation(context, fetchResponse);
    // LOGGER.debug("Forwarding original Fetch response");
    // context.forwardResponse(fetchResponse);
    // return (Void) null;
    // });
    // }
    // else {
    // applyTransformation(context, fetchResponse);
    // context.forwardResponse(fetchResponse);
    // }
    // }
    //
    // private void applyTransformation(KrpcFilterContext context, FetchResponseData responseData) {
    // for (FetchableTopicResponse topicData : responseData.responses()) {
    // for (PartitionData partitionData : topicData.partitions()) {
    // MemoryRecords records = (MemoryRecords) partitionData.records();
    // MemoryRecordsBuilder newRecords = NettyMemoryRecords.builder(context.allocate(records.sizeInBytes()), CompressionType.NONE,
    // TimestampType.CREATE_TIME, 0);
    //
    // for (MutableRecordBatch batch : records.batches()) {
    // for (Iterator<Record> batchRecords = batch.iterator(); batchRecords.hasNext();) {
    // Record batchRecord = batchRecords.next();
    // newRecords.append(batchRecord.timestamp(), batchRecord.key(), valueTransformation.transform(topicData.topic(), batchRecord.value()));
    // }
    // }
    //
    // partitionData.setRecords(newRecords.build());
    // }
    // }
    // }
}
