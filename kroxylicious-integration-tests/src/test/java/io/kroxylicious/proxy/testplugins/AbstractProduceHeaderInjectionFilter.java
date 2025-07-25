/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import io.kroxylicious.kafka.transform.RecordStream;
import io.kroxylicious.kafka.transform.RecordTransform;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Abstract {@link ProduceRequestFilter} implementation applying the abstract method pattern:
 * Subclasses implement {@link #headersToAdd(FilterContext)} to return
 * the headers to be added to the produced records.
 */
public abstract class AbstractProduceHeaderInjectionFilter implements ProduceRequestFilter {

    @NonNull
    protected abstract List<RecordHeader> headersToAdd(FilterContext context);

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion,
                                                                 RequestHeaderData header,
                                                                 ProduceRequestData request,
                                                                 FilterContext context) {
        List<RecordHeader> headers = headersToAdd(context);

        for (ProduceRequestData.TopicProduceData topicDatum : request.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionDatum : topicDatum.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionDatum.records();
                partitionDatum.setRecords(RecordStream.ofRecords(records)
                        .toMemoryRecords(
                                context.createByteBufferOutputStream(records.sizeInBytes()),
                                new RecordTransform<Void>() {
                                    @Override
                                    public void initBatch(RecordBatch batch) {

                                    }

                                    @Override
                                    public void init(@Nullable Void state, org.apache.kafka.common.record.Record record) {

                                    }

                                    @Override
                                    public void resetAfterTransform(Void state, org.apache.kafka.common.record.Record record) {

                                    }

                                    @Override
                                    public long transformOffset(org.apache.kafka.common.record.Record record) {
                                        return record.offset();
                                    }

                                    @Override
                                    public long transformTimestamp(org.apache.kafka.common.record.Record record) {
                                        return record.timestamp();
                                    }

                                    @Nullable
                                    @Override
                                    public ByteBuffer transformKey(org.apache.kafka.common.record.Record record) {
                                        return record.key();
                                    }

                                    @Nullable
                                    @Override
                                    public ByteBuffer transformValue(org.apache.kafka.common.record.Record record) {
                                        return record.value();
                                    }

                                    @Nullable
                                    @Override
                                    public Header[] transformHeaders(Record record) {
                                        return getHeaders(record, headers);
                                    }
                                }));
            }
        }
        return context.forwardRequest(header, request);
    }

    private Header[] getHeaders(Record record,
                                List<RecordHeader> headers) {
        List<Header> result = new ArrayList<>(record.headers().length + headers.size());
        result.addAll(Arrays.asList(record.headers()));
        result.addAll(headers);
        return result.toArray(new Header[0]);
    }
}
