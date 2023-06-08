/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
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
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;

/**
 * A sample ProduceRequestFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br />
 * This filter transforms the partition data sent by a Kafka producer in a produce request by replacing all
 * occurrences of the String "foo" with the String "bar". These strings are configurable in the config file,
 * so you could substitute this with any text you want.<br />
 * <br />
 * An example of a use case where this might be applicable is when producers are sending data to Kafka
 * using different formats from what consumers are expecting. You could configure this filter to transform
 * the data sent by producers to Kafka into the format consumers expect. In this example use case, the filter
 * could be further modified to apply different transformations to different topics, or when sent by
 * particular producers.
 */
public class SampleProduceRequestFilter implements ProduceRequestFilter {

    public static class SampleProduceRequestConfig extends BaseConfig {

        private final String from;
        private final String to;

        public SampleProduceRequestConfig(String from, String to) {
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

    public SampleProduceRequestFilter(SampleProduceRequestConfig config) {
        this.from = config.getFrom();
        this.to = config.getTo();
        this.timer = Timer
                .builder("sample_produce_request_filter_transform")
                .description("Time taken for the SampleProduceRequestFilter to transform the produce data.")
                .tag("filter", "SampleProduceRequestFilter")
                .register(Metrics.globalRegistry);
    }

    /**
     * Handle the given request, transforming the data in-place according to the configuration, and returning
     * the ProduceRequestData instance to be passed to the next filter.
     * @param apiVersion the apiVersion of the request
     * @param header request header.
     * @param request The KRPC message to handle.
     * @param context The context.
     */
    // TODO javadoc
    @Override
    public void onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        this.timer.record(() ->
        // We're timing this to report how long it takes through Micrometer
        applyTransformation(request, context));
        context.forwardRequest(header, request);
    }

    private void applyTransformation(ProduceRequestData request, KrpcFilterContext context) {
        request.topicData().forEach(topicData -> {
            for (PartitionProduceData partitionData : topicData.partitionData()) {
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
