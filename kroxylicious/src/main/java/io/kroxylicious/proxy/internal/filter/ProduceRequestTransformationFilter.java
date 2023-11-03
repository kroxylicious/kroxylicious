/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.internal.util.MemoryRecordsHelper;

/**
 * A filter for modifying the key/value/header/topic of {@link ApiKeys#PRODUCE} requests.
 */
public class ProduceRequestTransformationFilter implements ProduceRequestFilter {

    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    // TODO: add transformation support for key/header/topic

    public ProduceRequestTransformationFilter(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData data, FilterContext context) {
        applyTransformation(context, data);
        return context.forwardRequest(header, data);
    }

    private void applyTransformation(FilterContext ctx, ProduceRequestData req) {
        req.topicData().forEach(topicData -> {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                MemoryRecordsBuilder newRecords = MemoryRecordsHelper.builder(ctx.createByteBufferOutputStream(records.sizeInBytes()), CompressionType.NONE,
                        TimestampType.CREATE_TIME, 0);

                for (MutableRecordBatch batch : records.batches()) {
                    for (Iterator<Record> batchRecords = batch.iterator(); batchRecords.hasNext();) {
                        Record batchRecord = batchRecords.next();
                        newRecords.append(batchRecord.timestamp(), batchRecord.key(), valueTransformation.transform(topicData.name(), batchRecord.value()));
                    }
                }

                partitionData.setRecords(newRecords.build());
            }
        });
    }

}
