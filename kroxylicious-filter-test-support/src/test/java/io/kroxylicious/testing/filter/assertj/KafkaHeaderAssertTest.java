/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.testing.filter.assertj.Assertions.throwsAssertionErrorContaining;

class KafkaHeaderAssertTest {

    @Test
    void testHeaderHasKeyEqualTo() {
        RecordHeader header = new RecordHeader("foo", null);
        KafkaHeaderAssert headerAssert = KafkaHeaderAssert.assertThat(header);
        headerAssert.hasKeyEqualTo("foo");
        throwsAssertionErrorContaining(() -> headerAssert.hasKeyEqualTo("bar"), "[header key]");
        assertThrowsIfHeaderNull(nullAssert -> nullAssert.hasKeyEqualTo("any"));
    }

    @Test
    void testHeaderHasNullValue() {
        RecordHeader nullValue = new RecordHeader("foo", null);
        KafkaHeaderAssert nullValueAssert = KafkaHeaderAssert.assertThat(nullValue);

        RecordHeader nonNullValue = new RecordHeader("foo", new byte[]{ 1, 2, 3 });
        KafkaHeaderAssert nonNullValueAssert = KafkaHeaderAssert.assertThat(nonNullValue);

        nullValueAssert.hasNullValue();
        throwsAssertionErrorContaining(nonNullValueAssert::hasNullValue, "[header value]");
        assertThrowsIfHeaderNull(KafkaHeaderAssert::hasNullValue);
    }

    @Test
    void testHeaderHasValueEqualTo() {
        RecordHeader nullValue = new RecordHeader("foo", null);
        KafkaHeaderAssert nullValueAssert = KafkaHeaderAssert.assertThat(nullValue);

        RecordHeader nonNullValue = new RecordHeader("foo", "abc".getBytes(StandardCharsets.UTF_8));
        KafkaHeaderAssert nonNullValueAssert = KafkaHeaderAssert.assertThat(nonNullValue);

        nullValueAssert.hasValueEqualTo((String) null);
        nonNullValueAssert.hasValueEqualTo("abc");
        throwsAssertionErrorContaining(() -> nonNullValueAssert.hasValueEqualTo("other"), "[header value]");
        throwsAssertionErrorContaining(() -> nonNullValueAssert.hasValueEqualTo((String) null), "[header value]");
        throwsAssertionErrorContaining(() -> nullValueAssert.hasValueEqualTo("other"), "[header value]");
        assertThrowsIfHeaderNull(nullAssert -> nullAssert.hasValueEqualTo("any"));
    }

    @Test
    void testHeaderHasByteValue() {
        byte[] expectedBytes = "abc".getBytes(StandardCharsets.UTF_8);
        RecordHeader nonNullValue = new RecordHeader("foo", expectedBytes);
        KafkaHeaderAssert nonNullValueAssert = KafkaHeaderAssert.assertThat(nonNullValue);

        nonNullValueAssert.hasValueEqualTo(expectedBytes);
        throwsAssertionErrorContaining(() -> nonNullValueAssert.hasByteValueSatisfying(val -> org.assertj.core.api.Assertions.assertThat(val).isEmpty()),
                "[header value]");
        nonNullValueAssert.hasByteValueSatisfying(val -> org.assertj.core.api.Assertions.assertThat(val).isEqualTo(expectedBytes));
    }

    @Test
    void testHeaderHasStringValue() {
        String expectedStr = "abc";
        byte[] expectedBytes = expectedStr.getBytes(StandardCharsets.UTF_8);
        RecordHeader nonNullValue = new RecordHeader("foo", expectedStr.getBytes(StandardCharsets.UTF_8));
        KafkaHeaderAssert nonNullValueAssert = KafkaHeaderAssert.assertThat(nonNullValue);

        nonNullValueAssert.hasValueEqualTo(expectedBytes);
        throwsAssertionErrorContaining(() -> nonNullValueAssert.hasStringValueSatisfying(val -> org.assertj.core.api.Assertions.assertThat(val).isEmpty()),
                "[header value]");
        nonNullValueAssert.hasStringValueSatisfying(val -> org.assertj.core.api.Assertions.assertThat(val).isEqualTo(expectedStr));
    }

    void assertThrowsIfHeaderNull(ThrowingConsumer<KafkaHeaderAssert> action) {
        KafkaHeaderAssert headerAssert = KafkaHeaderAssert.assertThat(null);
        throwsAssertionErrorContaining(() -> action.accept(headerAssert), "[null header]");
    }

}
