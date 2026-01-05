/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A filter for modifying the key/value/header/topic of {@link ApiKeys#PRODUCE} requests.
 * <p>
 * <strong>Not intended to production use.</strong>
 * </p>
 */
class NewCachingProduceRequestTransformationFilter implements ProduceRequestFilter, ApiVersionsResponseFilter, TopicNameMapping {

    // downgrade to prevent produce requests with topic ids
    public static final ApiVersionsResponseTransformer DOWNGRADE_PRODUCE_API = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(
            Map.of(ApiKeys.PRODUCE, (short) 12));

    private final Map<Uuid, String> topicIdToNameCache = new HashMap<>();
    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    // TODO: add transformation support for key/header/topic

    NewCachingProduceRequestTransformationFilter(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData data, FilterContext context) {
        return applyTransformation(context, header, data);
    }

    private CompletionStage<RequestFilterResult> applyTransformation(FilterContext ctx, RequestHeaderData header, ProduceRequestData req) {
        List<Uuid> uuids = req.topicData().stream().map(ProduceRequestData.TopicProduceData::topicId).filter(uuid -> uuid != null && !uuid.equals(Uuid.ZERO_UUID))
                .toList();
        boolean allCached = uuids.stream().allMatch(topicIdToNameCache::containsKey);
        CompletionStage<TopicNameMapping> topicNameMappingCompletionStage = allCached ? CompletableFuture.completedStage(this) : lookupTopicIds(ctx,
                uuids);
        return topicNameMappingCompletionStage.thenCompose(topicNameMapping -> {
            if (topicNameMapping.anyFailures()) {
                return ctx.requestFilterResultBuilder().errorResponse(header, req, new UnknownServerException("failed to obtain topic ids")).completed();
            }
            req.topicData().forEach(topicData -> {
                for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                    MemoryRecords records = (MemoryRecords) partitionData.records();
                    var stream = ctx.createByteBufferOutputStream(records.sizeInBytes());
                    try (var newRecords = new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0,
                            System.currentTimeMillis(), RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                            RecordBatch.NO_PARTITION_LEADER_EPOCH,
                            stream.remaining())) {

                        for (MutableRecordBatch batch : records.batches()) {
                            for (Record batchRecord : batch) {
                                String topicName = topicData.name() != null && !topicData.name().isEmpty()
                                        ? topicData.name()
                                        : topicNameMapping.topicNames().get(topicData.topicId());
                                if (topicName == null || topicName.isEmpty()) {
                                    throw new IllegalStateException("topic name is null or empty");
                                }
                                newRecords.appendWithOffset(batchRecord.offset(), batchRecord.timestamp(), batchRecord.key(),
                                        valueTransformation.transform(topicData.name(), batchRecord.value()));
                            }
                        }

                        partitionData.setRecords(newRecords.build());
                    }
                }
            });
            return ctx.forwardRequest(header, req);
        });
    }

    @NonNull
    private CompletionStage<TopicNameMapping> lookupTopicIds(FilterContext ctx, List<Uuid> uuids) {
        return ctx.topicNames(uuids).whenComplete((topicNameMapping, throwable) -> {
            topicIdToNameCache.putAll(topicNameMapping.topicNames());
        });
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        return context.forwardResponse(header, DOWNGRADE_PRODUCE_API.transform(response));
    }

    @Override
    public boolean anyFailures() {
        return false;
    }

    @Override
    public Map<Uuid, String> topicNames() {
        return topicIdToNameCache;
    }

    @Override
    public Map<Uuid, TopicNameMappingException> failures() {
        return Map.of();
    }
}
