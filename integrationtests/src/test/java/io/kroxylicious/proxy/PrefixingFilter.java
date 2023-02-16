/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.internal.filter.FilterConfig;
import io.kroxylicious.proxy.internal.filter.FilterCreator;
import io.kroxylicious.proxy.internal.util.NettyMemoryRecords;

public class PrefixingFilter implements ProduceRequestFilter {

    private final String prefix;

    public static class PrefixingConfig extends FilterConfig {
        private final String prefix;

        public PrefixingConfig(String prefix) {
            this.prefix = prefix;
        }

        public String getPrefix() {
            return prefix;
        }
    }

    @FilterCreator
    public PrefixingFilter(PrefixingConfig config) {
        this.prefix = config.getPrefix();
    }

    @Override
    public void onProduceRequest(ProduceRequestData req, KrpcFilterContext ctx) {
        req.topicData().forEach(topicData -> {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                try (MemoryRecordsBuilder newRecords = NettyMemoryRecords.builder(ctx.allocate(records.sizeInBytes()), CompressionType.NONE,
                        TimestampType.CREATE_TIME, 0)) {

                    for (MutableRecordBatch batch : records.batches()) {
                        for (Record batchRecord : batch) {
                            String prefixed = prefix + new String(StandardCharsets.UTF_8.decode(batchRecord.value()).array());
                            ByteBuffer prefixedBuffer = ByteBuffer.wrap(prefixed.getBytes(StandardCharsets.UTF_8));
                            newRecords.append(batchRecord.timestamp(), batchRecord.key(), prefixedBuffer);
                        }
                    }

                    partitionData.setRecords(newRecords.build());
                }
            }
        });
        ctx.forwardRequest(req);
    }
}
