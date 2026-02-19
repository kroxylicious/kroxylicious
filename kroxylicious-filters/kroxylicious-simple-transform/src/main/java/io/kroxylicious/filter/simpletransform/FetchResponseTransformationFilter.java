/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

/**
 * A filter for modifying the key/value/header/topic of {@link ApiKeys#FETCH} responses.
 * <p>
 * <strong>Not intended to production use.</strong>
 * </p> */
class FetchResponseTransformationFilter implements FetchResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchResponseTransformationFilter.class);
    private static final EmptyMapping EMPTY_MAPPING = new EmptyMapping();

    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    FetchResponseTransformationFilter(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData fetchResponse,
                                                                 FilterContext context) {
        List<Uuid> uuids = fetchResponse.responses().stream().map(FetchResponseData.FetchableTopicResponse::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid)).toList();
        if (!uuids.isEmpty()) {
            return context.topicNames(uuids).thenCompose(topicNameMapping -> {
                applyTransformation(context, fetchResponse, topicNameMapping);
                return context.forwardResponse(header, fetchResponse);
            });
        }
        else {
            applyTransformation(context, fetchResponse, EMPTY_MAPPING);
            return context.forwardResponse(header, fetchResponse);
        }
    }

    private void applyTransformation(FilterContext context, FetchResponseData responseData, TopicNameMapping topicNameMapping) {
        for (FetchResponseData.FetchableTopicResponse topicData : responseData.responses()) {
            Optional<String> name = getName(topicNameMapping, topicData);
            if (name.isEmpty()) {
                LOGGER.debug("Failed to retrieve topicName for topicData with name: {} and topicId: {}, replacing all partitions with error responses",
                        topicData.topic(), topicData.topicId());
            }
            List<FetchResponseData.PartitionData> partitionData = topicData.partitions().stream().map(partition -> name.map(s -> transformRecords(context, partition, s))
                    .orElseGet(() -> partitionResponse(partition.partitionIndex(), Errors.UNKNOWN_SERVER_ERROR))).toList();
            topicData.setPartitions(partitionData);
        }
    }

    private FetchResponseData.PartitionData transformRecords(FilterContext context, FetchResponseData.PartitionData partitionData, String topicName) {
        MemoryRecords records = (MemoryRecords) partitionData.records();
        var stream = context.createByteBufferOutputStream(records.sizeInBytes());
        try (var newRecords = new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0,
                System.currentTimeMillis(), RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                stream.remaining())) {

            for (MutableRecordBatch batch : records.batches()) {
                for (Record batchRecord : batch) {
                    newRecords.appendWithOffset(batchRecord.offset(), batchRecord.timestamp(), batchRecord.key(),
                            valueTransformation.transform(topicName, batchRecord.value()));
                }
            }

            partitionData.setRecords(newRecords.build());
        }
        return partitionData;
    }

    /**
     * copied from {@link FetchResponse#partitionResponse(int, Errors)}.
     */
    public static FetchResponseData.PartitionData partitionResponse(int partition, Errors error) {
        return new FetchResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setErrorCode(error.code())
                .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
                .setRecords(MemoryRecords.EMPTY);
    }

    private static Optional<String> getName(TopicNameMapping topicNameMapping, FetchResponseData.FetchableTopicResponse topicData) {
        if (topicData.topic() != null && !topicData.topic().isEmpty()) {
            return Optional.of(topicData.topic());
        }
        else if (topicNameMapping.topicNames().containsKey(topicData.topicId())) {
            return Optional.of(topicNameMapping.topicNames().get(topicData.topicId()));
        }
        return Optional.empty();
    }

    private static class EmptyMapping implements TopicNameMapping {
        @Override
        public boolean anyFailures() {
            return false;
        }

        @Override
        public Map<Uuid, String> topicNames() {
            return Map.of();
        }

        @Override
        public Map<Uuid, TopicNameMappingException> failures() {
            return Map.of();
        }
    }
}
