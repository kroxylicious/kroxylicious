/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.internal.util.NettyMemoryRecords;

/**
 * An filter for modifying the key/value/header/topic of {@link ApiKeys#FETCH} responses.
 */
public class FetchResponseTransformationFilter implements FetchResponseFilter {

    @FunctionalInterface
    public interface ByteBufferTransformation {
        ByteBuffer transformation(ByteBuffer original);
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
    public void onFetchResponse(FetchResponseData responseData, KrpcFilterContext context) {
        applyTransformation(context, responseData);
        context.forwardResponse(responseData);
    }

    private void applyTransformation(KrpcFilterContext context, FetchResponseData responseData) {
        for (FetchableTopicResponse response : responseData.responses()) {
            for (PartitionData partitionData : response.partitions()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                MemoryRecordsBuilder newRecords = NettyMemoryRecords.builder(context.allocate(records.sizeInBytes()), CompressionType.NONE,
                        TimestampType.CREATE_TIME, 0);

                for (MutableRecordBatch batch : records.batches()) {
                    for (Iterator<Record> batchRecords = batch.iterator(); batchRecords.hasNext();) {
                        Record batchRecord = batchRecords.next();
                        newRecords.append(batchRecord.timestamp(), batchRecord.key(), valueTransformation.transformation(batchRecord.value()));
                    }
                }

                partitionData.setRecords(newRecords.build());
            }
        }
    }
}
