/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.test.assertj.Assertions.throwsAssertionErrorContaining;

class HeaderAssertTest {

    @Test
    void testHeaderHasKeyEqualTo() {
        RecordHeader header = new RecordHeader("foo", null);
        HeaderAssert headerAssert = KafkaAssertions.assertThat(header);
        headerAssert.hasKeyEqualTo("foo");
        throwsAssertionErrorContaining(() -> headerAssert.hasKeyEqualTo("bar"), "[header key]");
        assertThrowsIfHeaderNull(nullAssert -> nullAssert.hasKeyEqualTo("any"));
    }

    @Test
    void testHeaderHasNullValue() {
        RecordHeader nullValue = new RecordHeader("foo", null);
        HeaderAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);

        RecordHeader nonNullValue = new RecordHeader("foo", new byte[]{ 1, 2, 3 });
        HeaderAssert nonNullValueAssert = KafkaAssertions.assertThat(nonNullValue);

        nullValueAssert.hasNullValue();
        throwsAssertionErrorContaining(nonNullValueAssert::hasNullValue, "[header value]");
        assertThrowsIfHeaderNull(HeaderAssert::hasNullValue);
    }

    @Test
    void testHeaderHasValueEqualTo() {
        RecordHeader nullValue = new RecordHeader("foo", null);
        HeaderAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);

        RecordHeader nonNullValue = new RecordHeader("foo", "abc".getBytes(StandardCharsets.UTF_8));
        HeaderAssert nonNullValueAssert = KafkaAssertions.assertThat(nonNullValue);

        nullValueAssert.hasValueEqualTo((String) null);
        nonNullValueAssert.hasValueEqualTo("abc");
        throwsAssertionErrorContaining(() -> nonNullValueAssert.hasValueEqualTo("other"), "[header value]");
        throwsAssertionErrorContaining(() -> nonNullValueAssert.hasValueEqualTo((String) null), "[header value]");
        throwsAssertionErrorContaining(() -> nullValueAssert.hasValueEqualTo("other"), "[header value]");
        assertThrowsIfHeaderNull(nullAssert -> nullAssert.hasValueEqualTo("any"));
    }

    void assertThrowsIfHeaderNull(ThrowingConsumer<HeaderAssert> action) {
        HeaderAssert headerAssert = KafkaAssertions.assertThat((RecordHeader) null);
        throwsAssertionErrorContaining(() -> action.accept(headerAssert), "[null header]");
    }

}