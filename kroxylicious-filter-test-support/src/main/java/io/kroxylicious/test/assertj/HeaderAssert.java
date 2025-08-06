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
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;

@SuppressWarnings("UnusedReturnValue")
public class HeaderAssert extends AbstractAssert<HeaderAssert, Header> {

    private static final String VALUE_SUFFIX = "value";
    private static final String KEY_SUFFIX = "key";

    protected HeaderAssert(Header header) {
        super(header, HeaderAssert.class);
        describedAs(header == null ? "null header" : "header");
    }

    public static HeaderAssert assertThat(Header actual) {
        return new HeaderAssert(actual);
    }

    @SuppressWarnings("java:S1452")
    private AbstractStringAssert<?> key() {
        var existingDescription = descriptionText();
        return Assertions.assertThat(actual.key())
                .describedAs("%s %s", existingDescription, KEY_SUFFIX);
    }

    @SuppressWarnings("java:S1452")
    public AbstractByteArrayAssert<?> value() {
        var existingDescription = descriptionText();
        return Assertions.assertThat(actual.value())
                .describedAs("%s %s", existingDescription, VALUE_SUFFIX);
    }

    public HeaderAssert hasKeyEqualTo(String expected) {
        isNotNull().key().isEqualTo(expected);
        return this;
    }

    public HeaderAssert hasValueEqualTo(String expected) {
        if (expected == null) {
            isNotNull().value().isNull();
        }
        else {
            hasStringValueSatisfying(val -> Assertions.assertThat(val).isEqualTo(expected));
        }
        return this;
    }

    public HeaderAssert hasValueEqualTo(byte[] expected) {
        hasByteValueSatisfying(val -> Assertions.assertThat(val).isEqualTo(expected));
        return this;
    }

    public HeaderAssert hasNullValue() {
        isNotNull().value().isNull();
        return this;
    }

    public HeaderAssert hasStringValueSatisfying(ThrowingConsumer<String> assertion) {
        String existingDescription = descriptionText();
        isNotNull().value()
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .asString(StandardCharsets.UTF_8)
                .as("%s %s", existingDescription, VALUE_SUFFIX)
                .satisfies(assertion::accept);

        return this;
    }

    public HeaderAssert hasByteValueSatisfying(ThrowingConsumer<byte[]> assertion) {
        String existingDescription = descriptionText();
        isNotNull().value()
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .as("%s %s", existingDescription, VALUE_SUFFIX)
                .satisfies(assertion::accept);

        return this;
    }

}
