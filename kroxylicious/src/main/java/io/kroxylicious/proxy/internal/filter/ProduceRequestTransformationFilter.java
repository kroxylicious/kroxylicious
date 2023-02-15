/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.internal.util.NettyMemoryRecords;

/**
 * An filter for modifying the key/value/header/topic of {@link ApiKeys#PRODUCE} requests.
 */
public class ProduceRequestTransformationFilter implements ProduceRequestFilter {

    public static class UpperCasing implements ByteBufferTransformation {

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return ByteBuffer.wrap(new String(StandardCharsets.UTF_8.decode(in).array()).toUpperCase().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class ProduceRequestTransformationFilterConfig extends FilterConfig {

        private final String transformation;

        public ProduceRequestTransformationFilterConfig(String transformation) {
            this.transformation = transformation;
        }

        public String transformation() {
            return transformation;
        }
    }

    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    // TODO: add transformation support for key/header/topic

    public ProduceRequestTransformationFilter(ProduceRequestTransformationFilterConfig config) {
        try {
            this.valueTransformation = (ByteBufferTransformation) Class.forName(config.transformation()).getConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException
                | ClassNotFoundException e) {
            throw new IllegalArgumentException("Couldn't instantiate transformation class: " + config.transformation(), e);
        }
    }

    @Override
    public void onProduceRequest(ProduceRequestData data, KrpcFilterContext context) {
        applyTransformation(context, data);
        context.forwardRequest(data);
    }

    private void applyTransformation(KrpcFilterContext ctx, ProduceRequestData req) {
        req.topicData().forEach(topicData -> {
            for (PartitionProduceData partitionData : topicData.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                try (MemoryRecordsBuilder newRecords = ctx.recordsBuilder(records.sizeInBytes(), CompressionType.NONE,
                        TimestampType.CREATE_TIME, 0)) {

                    for (MutableRecordBatch batch : records.batches()) {
                        for (Iterator<Record> batchRecords = batch.iterator(); batchRecords.hasNext(); ) {
                            Record batchRecord = batchRecords.next();
                            newRecords.append(batchRecord.timestamp(), batchRecord.key(), valueTransformation.transform(topicData.name(), batchRecord.value()));
                        }
                    }

                    partitionData.setRecords(newRecords.build());
                }
            }
        });
    }
}
