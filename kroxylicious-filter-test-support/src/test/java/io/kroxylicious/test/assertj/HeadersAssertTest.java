/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class HeadersAssertTest {

    HeadersAssert headersAssert = KafkaAssertions.assertThat(new RecordHeaders()
            .add("foo", "1".getBytes(StandardCharsets.UTF_8))
            .add("foo", "2".getBytes(StandardCharsets.UTF_8))
            .add("bar", "3".getBytes(StandardCharsets.UTF_8)));
    private final HeadersAssert emptyAssert = KafkaAssertions.assertThat(new RecordHeaders());
    HeadersAssert singletonAssert = KafkaAssertions.assertThat(new RecordHeaders()
            .add("foo", "1".getBytes(StandardCharsets.UTF_8)));

    @Test
    void firstHeader() {
        Assertions.assertThatThrownBy(emptyAssert::firstHeader).isExactlyInstanceOf(AssertionError.class)
                .hasMessage("[headers] \n"
                        + "Expecting actual not to be empty");
        headersAssert.firstHeader().hasKeyEqualTo("foo").hasValueEqualTo("1");
    }

    @Test
    void lastHeader() {
        Assertions.assertThatThrownBy(emptyAssert::lastHeader).isExactlyInstanceOf(AssertionError.class)
                .hasMessage("[headers] \n"
                        + "Expecting actual not to be empty");
        headersAssert.lastHeader().hasKeyEqualTo("bar").hasValueEqualTo("3");
    }

    @Test
    void singleHeader() {
        Assertions.assertThatThrownBy(emptyAssert::singleHeader).isExactlyInstanceOf(AssertionError.class)
                .hasMessage("[headers] \n"
                        + "Expected size: 1 but was: 0 in:\n"
                        + "RecordHeaders(headers = [], isReadOnly = false)");
        Assertions.assertThatThrownBy(headersAssert::singleHeader).isInstanceOf(AssertionError.class);
        singletonAssert.singleHeader().hasValue();
    }

    @Test
    void firstHeaderWithKey() {
        headersAssert.firstHeaderWithKey("foo").hasKeyEqualTo("foo").hasValueEqualTo("1");
        headersAssert.firstHeaderWithKey("bar").hasKeyEqualTo("bar").hasValueEqualTo("3");
        Assertions.assertThatThrownBy(() -> headersAssert.firstHeaderWithKey("gee"))
                .isExactlyInstanceOf(AssertionError.class)
                .hasMessage("[headers with key gee] \n"
                        + "Expecting actual not to be empty");
    }

    @Test
    void lastHeaderWithKey() {
        headersAssert.lastHeaderWithKey("foo").hasKeyEqualTo("foo").hasValueEqualTo("2");
        headersAssert.lastHeaderWithKey("bar").hasKeyEqualTo("bar").hasValueEqualTo("3");
        Assertions.assertThatThrownBy(() -> headersAssert.lastHeaderWithKey("gee"))
                .isExactlyInstanceOf(AssertionError.class)
                .hasMessage("[headers with key gee] \n"
                        + "Expecting actual not to be empty");
    }

    @Test
    void singleHeaderWithKey() {
        headersAssert.singleHeaderWithKey("bar").hasKeyEqualTo("bar").hasValueEqualTo("3");
        Assertions.assertThatThrownBy(() -> headersAssert.singleHeaderWithKey("foo"))
                .isExactlyInstanceOf(AssertionError.class)
                .hasMessage("[headers with key foo] \n"
                        + "Expected size: 1 but was: 2 in:\n"
                        + "[RecordHeader(key = foo, value = [49]), RecordHeader(key = foo, value = [50])]");
    }
}
