/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * A filter for modifying the key/value/header/topic of {@link ApiKeys#FETCH} responses.
 * <p>
 * <strong>Not intended to production use.</strong>
 * </p> */
public class FetchResponseTransformationFilter implements FetchResponseFilter {

    // Version 12 was the first version that uses topic ids.
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = (short) 12;

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchResponseTransformationFilter.class);

    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    // TODO: add transformation support for key/header/topic

    public FetchResponseTransformationFilter(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(
            short apiVersion,
            ResponseHeaderData header,
            FetchResponseData fetchResponse,
            FilterContext context
    ) {
        List<MetadataRequestData.MetadataRequestTopic> requestTopics = fetchResponse.responses()
                                                                                    .stream()
                                                                                    .filter(t -> t.topic().isEmpty())
                                                                                    .map(fetchableTopicResponse -> {
                                                                                        Uuid uuid = fetchableTopicResponse.topicId();
                                                                                        return new MetadataRequestData.MetadataRequestTopic().setName(null)
                                                                                                                                             .setTopicId(uuid);
                                                                                    })
                                                                                    .distinct()
                                                                                    .toList();
        if (!requestTopics.isEmpty()) {
            LOGGER.debug("Fetch response contains {} unknown topic ids, lookup via Metadata request: {}", requestTopics.size(), requestTopics);
            var metadataHeader = new RequestHeaderData().setRequestApiVersion(METADATA_API_VER_WITH_TOPIC_ID_SUPPORT);
            var metadataRequest = new MetadataRequestData().setTopics(requestTopics);
            return context.<MetadataResponseData> sendRequest(metadataHeader, metadataRequest)
                          .thenCompose(metadataResponse -> {
                              Map<Uuid, String> uidToName = metadataResponse.topics()
                                                                            .stream()
                                                                            .collect(
                                                                                    Collectors.toMap(
                                                                                            MetadataResponseData.MetadataResponseTopic::topicId,
                                                                                            MetadataResponseData.MetadataResponseTopic::name
                                                                                    )
                                                                            );
                              LOGGER.debug("Metadata response yields {}, updating original Fetch response", uidToName);
                              for (var fetchableTopicResponse : fetchResponse.responses()) {
                                  fetchableTopicResponse.setTopic(uidToName.get(fetchableTopicResponse.topicId()));
                              }
                              applyTransformation(context, fetchResponse);
                              LOGGER.debug("Forwarding original Fetch response");

                              return context.responseFilterResultBuilder().forward(header, fetchResponse).completed();
                          });
        } else {
            applyTransformation(context, fetchResponse);
            return context.forwardResponse(header, fetchResponse);
        }
    }

    private void applyTransformation(FilterContext context, FetchResponseData responseData) {
        for (FetchResponseData.FetchableTopicResponse topicData : responseData.responses()) {
            for (FetchResponseData.PartitionData partitionData : topicData.partitions()) {
                var records = (MemoryRecords) partitionData.records();
                var stream = context.createByteBufferOutputStream(records.sizeInBytes());
                try (var newRecords = new MemoryRecordsBuilder(
                        stream,
                        RecordBatch.CURRENT_MAGIC_VALUE,
                        Compression.NONE,
                        TimestampType.CREATE_TIME,
                        0,
                        System.currentTimeMillis(),
                        RecordBatch.NO_PRODUCER_ID,
                        RecordBatch.NO_PRODUCER_EPOCH,
                        RecordBatch.NO_SEQUENCE,
                        false,
                        false,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        stream.remaining()
                )) {

                    for (MutableRecordBatch batch : records.batches()) {
                        for (Record batchRecord : batch) {
                            newRecords.appendWithOffset(
                                    batchRecord.offset(),
                                    batchRecord.timestamp(),
                                    batchRecord.key(),
                                    valueTransformation.transform(topicData.topic(), batchRecord.value())
                            );
                        }
                    }

                    partitionData.setRecords(newRecords.build());
                }
            }
        }
    }

}
