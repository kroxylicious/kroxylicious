/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractByteArrayAssert;
import org.assertj.core.api.Assertions;

public class HeaderAssert extends AbstractAssert<HeaderAssert, Header> {
    protected HeaderAssert(Header header) {
        super(header, HeaderAssert.class);
        describedAs(header == null ? "null header" : "header");
    }

    public static HeaderAssert assertThat(Header actual) {
        return new HeaderAssert(actual);
    }

    public HeaderAssert hasKeyEqualTo(String expected) {
        isNotNull();
        Assertions.assertThat(actual.key())
                .describedAs("header key")
                .isEqualTo(expected);
        return this;
    }

    public HeaderAssert hasValueEqualTo(String expected) {
        valueAssert().isEqualTo(expected == null ? null : expected.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    public HeaderAssert hasValueEqualTo(byte[] expected) {
        valueAssert().isEqualTo(expected);
        return this;
    }

    public HeaderAssert hasNullValue() {
        valueAssert().isNull();
        return this;
    }

    private AbstractByteArrayAssert<?> valueAssert() {
        isNotNull();
        AbstractByteArrayAssert<?> headerValue = Assertions.assertThat(actual.value())
                .describedAs("header value");
        return headerValue;
    }

    public AbstractByteArrayAssert<?> hasValue() {
        return Assertions.assertThat(actual.value());
    }
}
