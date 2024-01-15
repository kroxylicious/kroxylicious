/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectArrayAssert;

import static io.kroxylicious.test.assertj.KafkaAssertions.bufferToString;

public class RecordAssert extends AbstractAssert<RecordAssert, Record> {
    protected RecordAssert(Record record) {
        super(record, RecordAssert.class);
    }

    public static RecordAssert assertThat(Record actual) {
        return new RecordAssert(actual);
    }

    public RecordAssert hasOffsetEqualTo(int expect) {
        AbstractLongAssert<?> offset = offset();
        offset.isEqualTo(expect);
        return this;
    }

    private AbstractLongAssert<?> offset() {
        isNotNull();
        return Assertions.assertThat(actual.offset())
                .describedAs("record offset");
    }

    public RecordAssert hasTimestampEqualTo(int expect) {
        AbstractLongAssert<?> timestamp = timestamp();
        timestamp.isEqualTo(expect);
        return this;
    }

    private AbstractLongAssert<?> timestamp() {
        isNotNull();
        return Assertions.assertThat(actual.timestamp())
                .describedAs("record timestamp");
    }

    public RecordAssert hasKeyEqualTo(String expect) {
        isNotNull();
        var actualStr = bufferToString(actual.key());
        Assertions.assertThat(actualStr)
                .describedAs("record key")
                .isEqualTo(expect);
        return this;
    }

    public RecordAssert hasNullKey() {
        isNotNull();
        Assertions.assertThat(actual.key())
                .describedAs("record key")
                .isNull();
        return this;
    }

    public RecordAssert hasValueEqualTo(String expect) {
        isNotNull();
        var actualStr = bufferToString(actual.value());
        Assertions.assertThat(actualStr)
                .describedAs("record value")
                .isEqualTo(expect);
        return this;
    }

    public RecordAssert hasNullValue() {
        isNotNull();
        Assertions.assertThat(actual.value())
                .describedAs("record value")
                .isNull();
        return this;
    }

    public ObjectArrayAssert<Header> headers() {
        isNotNull();
        return Assertions.assertThat(actual.headers())
                .describedAs("record headers");
    }

    public RecordAssert hasEmptyHeaders() {
        headers().isEmpty();
        return this;
    }

    public RecordAssert hasHeadersSize(int expect) {
        headers().hasSize(expect);
        return this;
    }

    public RecordAssert containsHeaderWithKey(String expectedKey) {
        headers().anyMatch(h -> h.key().equals(expectedKey));
        return this;
    }

    public HeaderAssert firstHeader() {
        headers().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("first record header");
    }

    public HeaderAssert lastHeader() {
        headers().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[actual.headers().length - 1])
                .describedAs("last record header");
    }
}
