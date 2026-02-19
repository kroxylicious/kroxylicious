/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.transform.BatchAwareMemoryRecordsBuilder;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

/**
 * A filter for modifying the record value of {@link ApiKeys#PRODUCE} requests.
 * <p>
 * <strong>Not intended to production use.</strong>
 * </p>
 */
class ProduceRequestTransformationFilter implements ProduceRequestFilter {
    private static final int MAX_TOPIC_IDS_TO_LOG = 3;
    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceRequestTransformationFilter.class);

    ProduceRequestTransformationFilter(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData data, FilterContext context) {
        return applyTransformation(context, header, data);
    }

    private CompletionStage<RequestFilterResult> applyTransformation(FilterContext ctx, RequestHeaderData header, ProduceRequestData req) {
        List<Uuid> topicIds = req.topicData().stream()
                .map(ProduceRequestData.TopicProduceData::topicId)
                .filter(uuid -> !uuid.equals(Uuid.ZERO_UUID))
                .toList();
        return ctx.topicNames(topicIds).thenCompose(topicNameMapping -> {
            if (topicNameMapping.anyFailures()) {
                return maybeErrorResponse(ctx, header, req, topicNameMapping);
            }
            req.topicData().forEach(topicData -> {
                for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                    MemoryRecords records = (MemoryRecords) partitionData.records();
                    var stream = ctx.createByteBufferOutputStream(records.sizeInBytes());
                    BatchAwareMemoryRecordsBuilder builder = new BatchAwareMemoryRecordsBuilder(stream);

                    for (MutableRecordBatch batch : records.batches()) {
                        builder.addBatchLike(batch);
                        for (Record batchRecord : batch) {
                            String name = getName(topicNameMapping, topicData);
                            builder.appendWithOffset(batchRecord.offset(), batchRecord.timestamp(), batchRecord.key(),
                                    valueTransformation.transform(name, batchRecord.value()), batchRecord.headers());
                        }
                    }

                    partitionData.setRecords(builder.build());
                }
            });
            return ctx.forwardRequest(header, req);
        });
    }

    private static String getName(TopicNameMapping topicNameMapping, ProduceRequestData.TopicProduceData topicData) {
        if (topicData.name() != null && !topicData.name().isEmpty()) {
            return topicData.name();
        }
        else {
            return topicNameMapping.topicNames().get(topicData.topicId());
        }
    }

    private static CompletionStage<RequestFilterResult> maybeErrorResponse(FilterContext ctx, RequestHeaderData header, ProduceRequestData req,
                                                                           TopicNameMapping topicNameMapping) {

        if (req.acks() == 0) {
            logTopicLookupFailure(topicNameMapping, "dropping request without forwarding");
            return ctx.requestFilterResultBuilder().drop().completed();
        }
        else {
            logTopicLookupFailure(topicNameMapping, "responding with UNKNOWN_SERVER_ERROR");
            return ctx.requestFilterResultBuilder().errorResponse(header, req, Errors.UNKNOWN_SERVER_ERROR.exception()).completed();
        }
    }

    private static void logTopicLookupFailure(TopicNameMapping topicNameMapping, String outcome) {
        String firstThree = topicNameMapping.failures().keySet().stream().limit(MAX_TOPIC_IDS_TO_LOG).map(Objects::toString).collect(Collectors.joining(","));
        int failures = topicNameMapping.failures().size();
        boolean hasMore = failures > MAX_TOPIC_IDS_TO_LOG;
        LOGGER.atWarn()
                .setMessage("failed to map {} topic ids ({}{}) to names for acks=0 request, " + outcome + ".")
                .addArgument(() -> failures)
                .addArgument(() -> firstThree)
                .addArgument(() -> hasMore ? "..." : "")
                .log();
    }
}
