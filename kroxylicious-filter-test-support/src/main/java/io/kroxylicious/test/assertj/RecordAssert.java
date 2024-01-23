/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractByteArrayAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectArrayAssert;

import io.kroxylicious.test.record.RecordTestUtils;

public class RecordAssert extends AbstractAssert<RecordAssert, Record> {
    protected RecordAssert(Record record) {
        super(record, RecordAssert.class);
        describedAs(record == null ? "null record" : "record");
    }

    public static RecordAssert assertThat(Record actual) {
        return new RecordAssert(actual);
    }

    public RecordAssert hasOffsetEqualTo(int expect) {
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

    public RecordAssert hasTimestampEqualTo(int expect) {
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
                .describedAs("record key");
    }

    public RecordAssert hasKeyEqualTo(String expect) {
        isNotNull();
        Assertions.assertThat(actual).extracting(RecordTestUtils::recordKeyAsString)
                .describedAs("record key")
                .isEqualTo(expect);
        return this;
    }

    public RecordAssert hasNullKey() {
        isNotNull();
        keyStrAssert().isNull();
        return this;
    }

    private AbstractStringAssert<?> valueStrAssert() {
        isNotNull();
        return Assertions.assertThat(RecordTestUtils.recordValueAsString(actual))
                .describedAs("record value");
    }

    private AbstractByteArrayAssert<?> valueBytesAssert() {
        isNotNull();
        return Assertions.assertThat(RecordTestUtils.recordValueAsBytes(actual))
                .describedAs("record value");
    }

    public RecordAssert hasValueEqualTo(String expect) {
        isNotNull();
        valueStrAssert().isEqualTo(expect);
        return this;
    }

    public RecordAssert hasValueEqualTo(byte[] expect) {
        isNotNull();
        valueBytesAssert().isEqualTo(expect);
        return this;
    }

    public RecordAssert hasValueNotEqualTo(String notExpected) {
        isNotNull();
        valueStrAssert().isNotEqualTo(notExpected);
        return this;
    }

    public RecordAssert hasValueEqualTo(Record expected) {
        isNotNull();
        hasValueEqualTo(RecordTestUtils.recordValueAsBytes(expected));
        return this;
    }

    public RecordAssert hasNullValue() {
        isNotNull();
        Assertions.assertThat(actual).extracting(RecordTestUtils::recordValueAsString)
                .describedAs("record value")
                .isNull();
        return this;
    }

    public ObjectArrayAssert<Header> headersAssert() {
        isNotNull();
        return Assertions.assertThat(actual.headers())
                .describedAs("record headers");
    }

    public RecordAssert hasEmptyHeaders() {
        isNotNull();
        headersAssert().isEmpty();
        return this;
    }

    public HeaderAssert singleHeader() {
        isNotNull();
        headersAssert().singleElement();
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("record header");
    }

    public RecordAssert hasHeadersSize(int expect) {
        isNotNull();
        headersAssert().hasSize(expect);
        return this;
    }

    public RecordAssert containsHeaderWithKey(String expectedKey) {
        isNotNull();
        headersAssert().anyMatch(h -> h.key().equals(expectedKey));
        return this;
    }

    public HeaderAssert firstHeader() {
        isNotNull();
        headersAssert().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("first record header");
    }

    public HeaderAssert lastHeader() {
        isNotNull();
        headersAssert().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[actual.headers().length - 1])
                .describedAs("last record header");
    }
}
