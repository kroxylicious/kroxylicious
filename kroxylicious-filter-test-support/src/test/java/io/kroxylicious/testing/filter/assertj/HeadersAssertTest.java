/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.header.internals.RecordHeaders;

import static io.kroxylicious.testing.filter.assertj.Assertions.throwsAssertionErrorContaining;

class HeadersAssertTest {

    HeadersAssert headersAssert = KafkaAssertions.assertThat(new RecordHeaders()
            .add("foo", "1".getBytes(StandardCharsets.UTF_8))
            .add("foo", "2".getBytes(StandardCharsets.UTF_8))
            .add("bar", "3".getBytes(StandardCharsets.UTF_8)));
    private final HeadersAssert emptyAssert = KafkaAssertions.assertThat(new RecordHeaders());
    HeadersAssert singletonAssert = KafkaAssertions.assertThat(new RecordHeaders()
            .add("foo", null));

    @Test
    void firstHeader() {
        throwsAssertionErrorContaining(emptyAssert::firstHeader, "[headers]");
        headersAssert.firstHeader().hasKeyEqualTo("foo").hasValueEqualTo("1");
    }

    @Test
    void lastHeader() {
        throwsAssertionErrorContaining(emptyAssert::lastHeader, "[headers]");
        headersAssert.lastHeader().hasKeyEqualTo("bar").hasValueEqualTo("3");
    }

    @Test
    void singleHeader() {
        throwsAssertionErrorContaining(emptyAssert::singleHeader, "[headers]");
        throwsAssertionErrorContaining(headersAssert::singleHeader, "[headers]");
        singletonAssert.singleHeader().hasNullValue();
    }

    @Test
    void firstHeaderWithKey() {
        headersAssert.firstHeaderWithKey("foo").hasKeyEqualTo("foo").hasValueEqualTo("1");
        headersAssert.firstHeaderWithKey("bar").hasKeyEqualTo("bar").hasValueEqualTo("3");
        throwsAssertionErrorContaining(() -> headersAssert.firstHeaderWithKey("gee"), "[headers with key gee]");
    }

    @Test
    void lastHeaderWithKey() {
        headersAssert.lastHeaderWithKey("foo").hasKeyEqualTo("foo").hasValueEqualTo("2");
        headersAssert.lastHeaderWithKey("bar").hasKeyEqualTo("bar").hasValueEqualTo("3");
        throwsAssertionErrorContaining(() -> headersAssert.lastHeaderWithKey("gee"), "[headers with key gee]");
    }

    @Test
    void singleHeaderWithKey() {
        headersAssert.singleHeaderWithKey("bar").hasKeyEqualTo("bar").hasValueEqualTo("3");
        throwsAssertionErrorContaining(() -> headersAssert.singleHeaderWithKey("foo"), "[headers with key foo]");
    }

    @Test
    void nullHeaders() {
        HeadersAssert nullAssert = KafkaAssertions.assertThat((RecordHeaders) null);
        nullAssert.isNull();

        throwsAssertionErrorContaining(nullAssert::isNotNull, "[null headers]");
    }
}
