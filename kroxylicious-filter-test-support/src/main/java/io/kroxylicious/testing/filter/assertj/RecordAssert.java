/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractByteArrayAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectArrayAssert;

import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.testing.filter.record.RecordTestUtils;

/**
 * AssertJ assertions for {@link Record}.
 */
public class RecordAssert extends AbstractAssert<RecordAssert, Record> {

    private static final String RECORD_VALUE_DESCRIPTION = "record value";
    private static final String RECORD_KEY_DESCRIPTION = "record key";

    /**
     * Constructs an assertion for the given Record.
     *
     * @param record the actual Record
     */
    protected RecordAssert(Record record) {
        super(record, RecordAssert.class);
        describedAs(record == null ? "null record" : "record");
    }

    /**
     * Creates an assertion for the given Record.
     *
     * @param actual the actual Record
     * @return the assertion
     */
    public static RecordAssert assertThat(Record actual) {
        return new RecordAssert(actual);
    }

    /**
     * Verifies that the record has the expected offset.
     *
     * @param expect the expected offset
     * @return this assertion
     */
    public RecordAssert hasOffsetEqualTo(long expect) {
        isNotNull();
        AbstractLongAssert<?> offset = offsetAssert();
        offset.isEqualTo(expect);
        return this;
    }

    private AbstractLongAssert<?> offsetAssert() {
        isNotNull();
        return Assertions.assertThat(actual.offset())
                .describedAs("record offset");
    }

    /**
     * Verifies that the record has the expected timestamp.
     *
     * @param expect the expected timestamp
     * @return this assertion
     */
    public RecordAssert hasTimestampEqualTo(long expect) {
        isNotNull();
        AbstractLongAssert<?> timestamp = timestampAssert();
        timestamp.isEqualTo(expect);
        return this;
    }

    private AbstractLongAssert<?> timestampAssert() {
        isNotNull();
        return Assertions.assertThat(actual.timestamp())
                .describedAs("record timestamp");
    }

    private AbstractObjectAssert<?, String> keyStrAssert() {
        isNotNull();
        return Assertions.assertThat(actual).extracting(RecordTestUtils::recordKeyAsString)
                .describedAs(RECORD_KEY_DESCRIPTION);
    }

    /**
     * Verifies that the record's key, decoded as a UTF-8 string, is equal to the expected key.
     *
     * @param expect the expected key
     * @return this assertion
     */
    public RecordAssert hasKeyEqualTo(String expect) {
        isNotNull();
        Assertions.assertThat(actual).extracting(RecordTestUtils::recordKeyAsString)
                .describedAs(RECORD_KEY_DESCRIPTION)
                .isEqualTo(expect);
        return this;
    }

    /**
     * Verifies that the record's key is null.
     *
     * @return this assertion
     */
    public RecordAssert hasNullKey() {
        isNotNull();
        keyStrAssert().isNull();
        return this;
    }

    private AbstractStringAssert<?> valueStrAssert() {
        isNotNull();
        return Assertions.assertThat(RecordTestUtils.recordValueAsString(actual))
                .describedAs(RECORD_VALUE_DESCRIPTION);
    }

    private AbstractByteArrayAssert<?> valueBytesAssert() {
        isNotNull();
        return Assertions.assertThat(RecordTestUtils.recordValueAsBytes(actual))
                .describedAs(RECORD_VALUE_DESCRIPTION);
    }

    /**
     * Verifies that the record's value, decoded as a UTF-8 string, is equal to the expected value.
     *
     * @param expect the expected value
     * @return this assertion
     */
    public RecordAssert hasValueEqualTo(String expect) {
        isNotNull();
        valueStrAssert().isEqualTo(expect);
        return this;
    }

    /**
     * Verifies that the record's value is equal to the expected bytes.
     *
     * @param expect the expected value
     * @return this assertion
     */
    public RecordAssert hasValueEqualTo(byte[] expect) {
        isNotNull();
        valueBytesAssert().isEqualTo(expect);
        return this;
    }

    /**
     * Verifies that the record's value, decoded as a UTF-8 string, is not equal to the given value.
     *
     * @param notExpected the value the record must not have
     * @return this assertion
     */
    public RecordAssert hasValueNotEqualTo(String notExpected) {
        isNotNull();
        valueStrAssert().isNotEqualTo(notExpected);
        return this;
    }

    /**
     * Verifies that the record's value is equal to the value of the given record.
     *
     * @param expected the record with the expected value
     * @return this assertion
     */
    public RecordAssert hasValueEqualTo(Record expected) {
        isNotNull();
        hasValueEqualTo(RecordTestUtils.recordValueAsBytes(expected));
        return this;
    }

    /**
     * Verifies that the record's value is null.
     *
     * @return this assertion
     */
    public RecordAssert hasNullValue() {
        isNotNull();
        Assertions.assertThat(actual).extracting(RecordTestUtils::recordValueAsString)
                .describedAs(RECORD_VALUE_DESCRIPTION)
                .isNull();
        return this;
    }

    /**
     * Creates an assertion for the record's headers.
     *
     * @return the headers assertion
     */
    public ObjectArrayAssert<Header> headersAssert() {
        isNotNull();
        return Assertions.assertThat(actual.headers())
                .describedAs("record headers");
    }

    /**
     * Verifies that the record has no headers.
     *
     * @return this assertion
     */
    public RecordAssert hasEmptyHeaders() {
        isNotNull();
        headersAssert().isEmpty();
        return this;
    }

    /**
     * Verifies that the record has exactly one header and creates an assertion for it.
     *
     * @return the single header assertion
     */
    public HeaderAssert singleHeader() {
        isNotNull();
        headersAssert().hasSize(1);
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("record header");
    }

    /**
     * Verifies that the record has the expected number of headers.
     *
     * @param expect the expected number of headers
     * @return this assertion
     */
    public RecordAssert hasHeadersSize(int expect) {
        isNotNull();
        headersAssert().hasSize(expect);
        return this;
    }

    /**
     * Verifies that the record has at least one header with the given key.
     *
     * @param expectedKey the expected header key
     * @return this assertion
     */
    public RecordAssert containsHeaderWithKey(String expectedKey) {
        isNotNull();
        headersAssert().anyMatch(h -> h.key().equals(expectedKey));
        return this;
    }

    /**
     * Verifies that the record has headers and creates an assertion for the first one.
     *
     * @return the first header assertion
     */
    public HeaderAssert firstHeader() {
        isNotNull();
        headersAssert().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("first record header");
    }

    /**
     * Verifies that the record has headers and creates an assertion for the last one.
     *
     * @return the last header assertion
     */
    public HeaderAssert lastHeader() {
        isNotNull();
        headersAssert().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[actual.headers().length - 1])
                .describedAs("last record header");
    }
}
