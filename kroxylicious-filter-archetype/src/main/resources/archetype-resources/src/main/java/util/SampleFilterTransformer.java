package ${package}.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.kroxylicious.proxy.filter.FilterContext;
import ${package}.config.SampleFilterConfig;

/**
 * Transformer class for the sample filters. Provides static transform functions for find-and-replace
 * transformation of data in ProduceRequests and FetchResponses.
 */
public class SampleFilterTransformer {

    /**
     * Transforms the given partition data according to the provided configuration.
     * @param partitionData the partition data to be transformed
     * @param context the context
     * @param config the transform configuration
     */
    public static void transform(ProduceRequestData.PartitionProduceData partitionData, FilterContext context, SampleFilterConfig config) {
        partitionData.setRecords(transformPartitionRecords((AbstractRecords) partitionData.records(), context, config.getFindValue(), config.getReplacementValue()));
    }

    /**
     * Transforms the given partition data according to the provided configuration.
     * @param partitionData the partition data to be transformed
     * @param context the context
     * @param config the transform configuration
     */
    public static void transform(FetchResponseData.PartitionData partitionData, FilterContext context, SampleFilterConfig config) {
        partitionData.setRecords(transformPartitionRecords((AbstractRecords) partitionData.records(), context, config.getFindValue(), config.getReplacementValue()));
    }

    /**
     * Performs find-and-replace transformations on the given partition records.
     * @param records the partition records to be transformed
     * @param context the context
     * @param findValue the value to be replaced
     * @param replacementValue the replacement value
     * @return the transformed partition records
     */
    private static AbstractRecords transformPartitionRecords(AbstractRecords records, FilterContext context, String findValue, String replacementValue) {
        if (records.batchIterator().hasNext()) {
            ByteBufferOutputStream stream = context.createByteBufferOutputStream(records.sizeInBytes());
            MemoryRecordsBuilder newRecords = createMemoryRecordsBuilder(stream, records.firstBatch());

            for (RecordBatch batch : records.batches()) {
                for (Record batchRecord : batch) {
                    newRecords.appendWithOffset(batchRecord.offset(), batchRecord.timestamp(), batchRecord.key(),
                            transformRecord(batchRecord.value(), findValue, replacementValue),
                            batchRecord.headers());
                }
            }
            return newRecords.build();
        }
        return records;
    }

    /**
     * Performs a find-and-replace transformation of a given record value.
     * @param in the record value to be transformed
     * @param findValue the value to be replaced
     * @param replacementValue the replacement value
     * @return the transformed record value
     */
    private static ByteBuffer transformRecord(ByteBuffer in, String findValue, String replacementValue) {
        return ByteBuffer.wrap(new String(StandardCharsets.UTF_8.decode(in).array()).replaceAll(findValue, replacementValue).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Instantiates a MemoryRecordsBuilder object using the given stream. This duplicates some of the
     * functionality in io.kroxylicious.proxy.internal, but we aren't supposed to import from there.
     */
    private static MemoryRecordsBuilder createMemoryRecordsBuilder(ByteBufferOutputStream stream, RecordBatch firstBatch) {
        return new MemoryRecordsBuilder(stream, firstBatch.magic(), Compression.of(firstBatch.compressionType()).build(), firstBatch.timestampType(),
                firstBatch.baseOffset(),
                firstBatch.maxTimestamp(), firstBatch.producerId(), firstBatch.producerEpoch(), firstBatch.baseSequence(), firstBatch.isTransactional(),
                firstBatch.isControlBatch(), firstBatch.partitionLeaderEpoch(), stream.remaining());
    }
}
