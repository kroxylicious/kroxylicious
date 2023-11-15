/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import com.dylibso.chicory.runtime.ExportFunction;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Memory;
import com.dylibso.chicory.runtime.Module;
import com.dylibso.chicory.wasm.types.Value;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.sample.config.SampleFilterConfig;

/**
 * Transformer class for the sample filters. Provides static transform functions for find-and-replace
 * transformation of data in ProduceRequests and FetchResponses.
 */
public class SampleWasmFilterTransformer {

    /**
     * Transforms the given partition data according to the provided configuration.
     * @param partitionData the partition data to be transformed
     * @param context the context
     * @param config the transform configuration
     */
    public static void transform(ProduceRequestData.PartitionProduceData partitionData, FilterContext context, SampleFilterConfig config) {
        partitionData.setRecords(transformPartitionRecords((AbstractRecords) partitionData.records(), context, config.getReplacerModule()));
    }

    /**
     * Transforms the given partition data according to the provided configuration.
     * @param partitionData the partition data to be transformed
     * @param context the context
     * @param config the transform configuration
     */
    public static void transform(FetchResponseData.PartitionData partitionData, FilterContext context, SampleFilterConfig config) {
        partitionData.setRecords(transformPartitionRecords((AbstractRecords) partitionData.records(), context, config.getReplacerModule()));
    }

    /**
     * Performs find-and-replace transformations on the given partition records.
     * @param records the partition records to be transformed
     * @param context the context
     * @param replacerModule the wasm replacer module to be used
     * @return the transformed partition records
     */
    private static AbstractRecords transformPartitionRecords(AbstractRecords records, FilterContext context, String replacerModule) {
        if (records.batchIterator().hasNext()) {
            ByteBufferOutputStream stream = context.createByteBufferOutputStream(records.sizeInBytes());
            MemoryRecordsBuilder newRecords = createMemoryRecordsBuilder(stream, records.firstBatch());

            for (RecordBatch batch : records.batches()) {
                for (Record batchRecord : batch) {
                    newRecords.append(batchRecord.timestamp(), batchRecord.key(), transformRecord(batchRecord.value(), replacerModule),
                            batchRecord.headers());
                }
            }
            return newRecords.build();
        }
        return records;
    }

    // Hacked around caching of last used Wasm Module
    private static String currentWasmModule;
    private static Instance currentWasmInstance;
    private static ExportFunction replacerFunction;
    private static ExportFunction alloc;
    private static ExportFunction dealloc;
    private static Memory memory;

    /**
     * Performs a find-and-replace transformation of a given record value.
     * @param in the record value to be transformed
     * @param replacerModule the value to be replaced
     * @return the transformed record value
     */
    private static ByteBuffer transformRecord(ByteBuffer in, String replacerModule) {
        if (currentWasmModule == null || !currentWasmModule.equals(replacerModule)) {
            // TODO: here we are making strong assumptions on where the compiled wasm modules will be placed
            try (InputStream moduleInputStream = SampleWasmFilterTransformer.class.getResourceAsStream("/wasm/" + replacerModule)) {
                currentWasmInstance = Module.build(moduleInputStream).instantiate();
                replacerFunction = currentWasmInstance.getExport("replace");
                alloc = currentWasmInstance.getExport("alloc");
                dealloc = currentWasmInstance.getExport("dealloc");
                memory = currentWasmInstance.getMemory();
                currentWasmModule = replacerModule;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        String originalString = new String(StandardCharsets.UTF_8.decode(in).array());

        int len = originalString.getBytes().length;
        int ptr = alloc.apply(Value.i32(len))[0].asInt();
        memory.put(ptr, originalString);
        int outStringSize = replacerFunction.apply(Value.i32(ptr), Value.i32(len))[0].asInt();
        String outString = memory.getString(ptr, outStringSize);
        dealloc.apply(Value.i32(ptr), Value.i32(len));

        return ByteBuffer.wrap(outString.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Instantiates a MemoryRecordsBuilder object using the given stream. This duplicates some of the
     * functionality in io.kroxylicious.proxy.internal, but we aren't supposed to import from there.
     */
    private static MemoryRecordsBuilder createMemoryRecordsBuilder(ByteBufferOutputStream stream, RecordBatch firstBatch) {
        return new MemoryRecordsBuilder(stream, firstBatch.magic(), firstBatch.compressionType(), firstBatch.timestampType(), firstBatch.baseOffset(),
                firstBatch.maxTimestamp(), firstBatch.producerId(), firstBatch.producerEpoch(), firstBatch.baseSequence(), firstBatch.isTransactional(),
                firstBatch.isControlBatch(), firstBatch.partitionLeaderEpoch(), stream.remaining());
    }
}
