package ${package}.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import io.kroxylicious.kafka.transform.RecordTransform;
import ${package}.config.SampleFilterConfig;

/**
 * A {@link RecordTransform} implementation used by the sample filters to perform find-and-replace
 * on the UTF-8 encoded value of each Kafka {@link Record}.
 *
 * <p>Instances are stateful and follow the {@link RecordTransform} lifecycle:
 * <ol>
 * <li>
 *     {@link SampleFilterTransformer#init} is called first to pre-compute the transformed value.
 *     This is where the find-and-replace occurs.
 * </li>
 * <li>
 *     {@code SampleFilterTransformer#transform*()} methods are then called to supply the fields
 *     of the new record (offset, timestamp, key, value, and headers).
 * </li>
 * <li>
 *     {@link SampleFilterTransformer#resetAfterTransform} is called last to clear internal state
 *     and ready the instance for the next record.
 * </li>
 * </ol>
 *
 * In this implementation, only the record value is modified. Offset, timestamp, key, and headers
 * are passed through unchanged.
 */
public class SampleFilterTransformer implements RecordTransform<Void> {

    private final String findValue;
    private final String replacementValue;
    private ByteBuffer transformedValue;

    /**
     * Creates a transformer that replaces all occurrences of {@code findValue} with
     * {@code replacementValue} in the UTF-8 encoded value of each Kafka {@link Record}.
     *
     * @param findValue the regex pattern to search for in the record value
     * @param replacementValue the string to substitute for each match
     */
    public SampleFilterTransformer(String findValue, String replacementValue) {
        this.findValue = findValue;
        this.replacementValue = replacementValue;
    }

    /**
     * Called once per {@link RecordBatch} before records in the batch are processed.
     * This transformer does not need batch-level state, so this is a no-op.
     */
    @Override
    public void initBatch(RecordBatch batch) {}

    /**
     * Called once per record before any {@code transform*()} methods are invoked.
     * Applies the find-and-replace to the record's value and caches the result so
     * that {@link #transformValue(Record)} can be called repeatedly without repeating the work.
     * Records with a {@code null} value are handled gracefully by storing {@code null}.
     *
     * @param state unused — this transformer requires no external state
     * @param record the record about to be transformed
     */
    @Override
    public void init(Void state, Record record) {
        ByteBuffer value = record.value();
        if (value != null) {
            String original = StandardCharsets.UTF_8.decode(value.duplicate()).toString();
            transformedValue = ByteBuffer.wrap(original.replaceAll(findValue, replacementValue).getBytes(StandardCharsets.UTF_8));
        }
        else {
            transformedValue = null;
        }
    }

    /**
     * Returns the record's offset unchanged. This sample does not modify offsets.
     *
     * @param record the record being transformed
     * @return the original record offset
     */
    @Override
    public long transformOffset(Record record) {
        return record.offset();
    }

    /**
     * Returns the record's timestamp unchanged. This sample does not modify timestamps.
     *
     * @param record the record being transformed
     * @return the original record timestamp
     */
    @Override
    public long transformTimestamp(Record record) {
        return record.timestamp();
    }

    /**
     * Returns the record's key unchanged. This sample only transforms record values.
     *
     * @param record the record being transformed
     * @return the original record key
     */
    @Override
    public ByteBuffer transformKey(Record record) {
        return record.key();
    }

    /**
     * Returns the transformed record value computed by {@link SampleFilterTransformer#init}.
     * A duplicate of the transformed record value buffer is returned leaving the original
     * for reuse by {@link SampleFilterTransformer}.
     *
     * @param record the record being transformed
     * @return the find-and-replace result, or {@code null} if the original record value was {@code null}
     */
    @Override
    public ByteBuffer transformValue(Record record) {
        return transformedValue == null ? null : transformedValue.duplicate();
    }

    /**
     * Returns the record's headers unchanged. This sample does not modify headers.
     *
     * @param record the record being transformed
     * @return the original record headers
     */
    @Override
    public Header[] transformHeaders(Record record) {
        return record.headers();
    }

    /**
     * Called after all {@code transform*()} methods have been invoked for a record.
     * Clears the buffer containing the transformed record value in order to prepare the
     * transformer for the next record.
     *
     * @param state unused — this transformer requires no external state
     * @param record the record that was just transformed
     */
    @Override
    public void resetAfterTransform(Void state, Record record) {
        if (transformedValue != null) {
            transformedValue.clear();
        }
    }
}
