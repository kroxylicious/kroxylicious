/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.interceptor;

import java.nio.ByteBuffer;
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

import io.strimzi.kproxy.codec.DecodedRequestFrame;

/**
 * An interceptor for modifying the key/value/header/topic of {@link ApiKeys#PRODUCE} requests.
 */
public class ProduceRecordTransformationInterceptor implements Interceptor {

    @FunctionalInterface
    public interface ByteBufferTransformation {
        ByteBuffer transformation(ByteBuffer original);
    }

    /**
     * Transformation to be applied to record value.
     */
    private final ByteBufferTransformation valueTransformation;

    public ProduceRecordTransformationInterceptor(ByteBufferTransformation valueTransformation) {
        this.valueTransformation = valueTransformation;
    }

    @Override
    public boolean shouldDecodeRequest(ApiKeys apiKey, int apiVersion) {
        return apiKey == ApiKeys.PRODUCE;
    }

    @Override
    public boolean shouldDecodeResponse(ApiKeys apiKey, int apiVersion) {
        return false;
    }

    @Override
    public RequestHandler requestHandler() {
        return new RequestHandler() {

            @Override
            public DecodedRequestFrame handleRequest(DecodedRequestFrame requestFrame) {
                var req = (ProduceRequestData) requestFrame.body();

                req.topicData().forEach(tpd -> {
                    for (PartitionProduceData partitionData : tpd.partitionData()) {
                        MemoryRecords records = (MemoryRecords) partitionData.records();
                        MemoryRecordsBuilder newRecords = MemoryRecords.builder(ByteBuffer.allocate(records.sizeInBytes()), CompressionType.NONE,
                                TimestampType.CREATE_TIME, 0);

                        for (MutableRecordBatch batch : records.batches()) {
                            for (Iterator<Record> batchRecords = batch.iterator(); batchRecords.hasNext();) {
                                Record batchRecord = batchRecords.next();
                                newRecords.append(batchRecord.timestamp(), batchRecord.key(), valueTransformation.transformation(batchRecord.value()));
                            }
                        }

                        partitionData.setRecords(newRecords.build());
                    }
                });

                return requestFrame;
            }
        };
    }

    @Override
    public ResponseHandler responseHandler() {
        return null;
    }
}
