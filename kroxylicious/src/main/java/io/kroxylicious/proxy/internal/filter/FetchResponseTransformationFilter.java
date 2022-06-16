/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.internal.util.NettyMemoryRecords;

/**
 * An filter for modifying the key/value/header/topic of {@link ApiKeys#FETCH} responses.
 */
public class FetchResponseTransformationFilter implements FetchResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchResponseTransformationFilter.class);

    @FunctionalInterface
    public interface ByteBufferTransformation {
        ByteBuffer transformation(String topicName, ByteBuffer original);
    }

    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    // TODO: add transformation support for key/header/topic

    public FetchResponseTransformationFilter(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public void onFetchResponse(FetchResponseData fetchResponse, KrpcFilterContext context) {
        List<MetadataRequestData.MetadataRequestTopic> requestTopics = fetchResponse.responses().stream()
                .filter(t -> t.topic().isEmpty())
                .map(fetchableTopicResponse -> {
                    Uuid uuid = fetchableTopicResponse.topicId();
                    return new MetadataRequestData.MetadataRequestTopic().setName(null).setTopicId(uuid);
                })
                .distinct()
                .collect(Collectors.toList());
        if (!requestTopics.isEmpty()) {
            LOGGER.debug("Fetch response contains {} unknown topic ids, lookup via Metadata request: {}", requestTopics.size(), requestTopics);
            // TODO Can't necessarily use HIGHEST_SUPPORTED_VERSION, must use highest supported version
            context.<MetadataResponseData> sendRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
                    new MetadataRequestData()
                            .setTopics(requestTopics))
                    .map(metadataResponse -> {
                        Map<Uuid, String> uidToName = metadataResponse.topics().stream().collect(Collectors.toMap(ti -> ti.topicId(), ti -> ti.name()));
                        LOGGER.debug("Metadata response yields {}, updating original Fetch response", uidToName);
                        for (var fetchableTopicResponse : fetchResponse.responses()) {
                            fetchableTopicResponse.setTopic(uidToName.get(fetchableTopicResponse.topicId()));
                        }
                        applyTransformation(context, fetchResponse);
                        LOGGER.debug("Forwarding original Fetch response");
                        context.forwardResponse(fetchResponse);
                        return (Void) null;
                    });
        }
        else {
            applyTransformation(context, fetchResponse);
            context.forwardResponse(fetchResponse);
        }
    }

    private void applyTransformation(KrpcFilterContext context, FetchResponseData responseData) {
        for (FetchableTopicResponse topicData : responseData.responses()) {
            for (PartitionData partitionData : topicData.partitions()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                MemoryRecordsBuilder newRecords = NettyMemoryRecords.builder(context.allocate(records.sizeInBytes()), CompressionType.NONE,
                        TimestampType.CREATE_TIME, 0);

                for (MutableRecordBatch batch : records.batches()) {
                    for (Iterator<Record> batchRecords = batch.iterator(); batchRecords.hasNext();) {
                        Record batchRecord = batchRecords.next();
                        newRecords.append(batchRecord.timestamp(), batchRecord.key(), valueTransformation.transformation(topicData.topic(), batchRecord.value()));
                    }
                }

                partitionData.setRecords(newRecords.build());
            }
        }
    }
}
