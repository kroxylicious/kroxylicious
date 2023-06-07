/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;

// TODO javadoc

/**
 *
 */
public class SampleFetchResponseFilter implements FetchResponseFilter {

    public static class SampleFetchResponseConfig extends BaseConfig {

        private final String from;
        private final String to;

        public SampleFetchResponseConfig(String from, String to) {
            this.from = from;
            this.to = to;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }
    }

    private final String from;
    private final String to;
    private final Timer timer;

    public SampleFetchResponseFilter(SampleFetchResponseConfig config) {
        this.from = config.getFrom();
        this.to = config.getTo();
        this.timer = Timer
                .builder("sample_fetch_response_filter_transform")
                .description("Time taken for the SampleFetchResponseFilter to transform the produce data.")
                .tag("filter", "SampleFetchResponseFilter")
                .register(Metrics.globalRegistry);
    }

    // TODO javadoc
    @Override
    public void onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        this.timer.record(() ->
        // We're timing this to report how long it takes through Micrometer
        applyTransformation(response, context));
        context.forwardResponse(header, response);
    }

    private void applyTransformation(FetchResponseData response, KrpcFilterContext context) {
        response.responses().forEach(responseData -> {
            for (FetchResponseData.PartitionData partitionData : responseData.partitions()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                ByteBufferOutputStream stream = context.createByteBufferOutputStream(records.sizeInBytes());
                MemoryRecordsBuilder newRecords = createMemoryRecordsBuilder(stream);

                for (MutableRecordBatch batch : records.batches()) {
                    for (Record batchRecord : batch) {
                        newRecords.append(batchRecord.timestamp(), batchRecord.key(), transform(batchRecord.value()));
                    }
                }

                partitionData.setRecords(newRecords.build());
            }
        });
    }

    private ByteBuffer transform(ByteBuffer in) {
        return ByteBuffer.wrap(new String(StandardCharsets.UTF_8.decode(in).array()).replaceAll(this.from, this.to).getBytes(StandardCharsets.UTF_8));
    }

    // Reinventing the wheel a bit here to avoid importing from io.kroxylicious.proxy.internal and to improve readability
    private static MemoryRecordsBuilder createMemoryRecordsBuilder(ByteBufferOutputStream stream) {
        return new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0, RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH,
                stream.remaining());
    }
}
