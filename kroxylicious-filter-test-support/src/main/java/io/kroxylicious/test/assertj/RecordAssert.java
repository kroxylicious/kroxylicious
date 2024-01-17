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
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectArrayAssert;

import io.kroxylicious.test.record.RecordTestUtils;

public class RecordAssert extends AbstractAssert<RecordAssert, Record> {
    protected RecordAssert(Record record) {
        super(record, RecordAssert.class);
    }

    public static RecordAssert assertThat(Record actual) {
        return new RecordAssert(actual);
    }

    public RecordAssert hasOffsetEqualTo(int expect) {
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
        return Assertions.assertThat(actual).extracting(RecordTestUtils::recordKeyAsString)
                .describedAs("record key");
    }

    public RecordAssert hasKeyEqualTo(String expect) {
        Assertions.assertThat(actual).extracting(RecordTestUtils::recordKeyAsString)
                .describedAs("record key")
                .isEqualTo(expect);
        return this;
    }

    public RecordAssert hasNullKey() {
        keyStrAssert()
                .isNull();
        return this;
    }

    private AbstractObjectAssert<?, String> valueStrAssert() {
        return Assertions.assertThat(actual).extracting(RecordTestUtils::recordValueAsString)
                .describedAs("record value");
    }

    public RecordAssert hasValueEqualTo(String expect) {
        valueStrAssert().isEqualTo(expect);
        return this;
    }

    public RecordAssert hasNullValue() {
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
        headersAssert().isEmpty();
        return this;
    }

    public HeaderAssert singleHeader() {
        headersAssert().singleElement();
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("record header");
    }

    public RecordAssert hasHeadersSize(int expect) {
        headersAssert().hasSize(expect);
        return this;
    }

    public RecordAssert containsHeaderWithKey(String expectedKey) {
        headersAssert().anyMatch(h -> h.key().equals(expectedKey));
        return this;
    }

    public HeaderAssert firstHeader() {
        headersAssert().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[0])
                .describedAs("first record header");
    }

    public HeaderAssert lastHeader() {
        headersAssert().isNotEmpty();
        return HeaderAssert.assertThat(actual.headers()[actual.headers().length - 1])
                .describedAs("last record header");
    }
}
