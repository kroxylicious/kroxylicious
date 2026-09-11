/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractByteArrayAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;

import io.kroxylicious.kafka.common.header.Header;

/**
 * AssertJ assertions for {@link Header}.
 */
@SuppressWarnings("UnusedReturnValue")
public class HeaderAssert extends AbstractAssert<HeaderAssert, Header> {

    private static final String VALUE_SUFFIX = "value";
    private static final String KEY_SUFFIX = "key";
    /** Format pattern used to compose assertion descriptions from an existing description and a suffix. */
    public static final String DESCRIBED_AS_PATTERN = "%s %s";

    /**
     * Constructs an assertion for the given Header.
     *
     * @param header the actual Header
     */
    protected HeaderAssert(Header header) {
        super(header, HeaderAssert.class);
        describedAs(header == null ? "null header" : "header");
    }

    /**
     * Creates an assertion for the given Header.
     *
     * @param actual the actual Header
     * @return the assertion
     */
    public static HeaderAssert assertThat(Header actual) {
        return new HeaderAssert(actual);
    }

    @SuppressWarnings("java:S1452")
    private AbstractStringAssert<?> key() {
        var existingDescription = descriptionText();
        return Assertions.assertThat(actual.key())
                .describedAs(DESCRIBED_AS_PATTERN, existingDescription, KEY_SUFFIX);
    }

    /**
     * Creates an assertion for the header's value.
     *
     * @return the value assertion
     */
    @SuppressWarnings("java:S1452")
    public AbstractByteArrayAssert<?> value() {
        var existingDescription = descriptionText();
        return Assertions.assertThat(actual.value())
                .describedAs(DESCRIBED_AS_PATTERN, existingDescription, VALUE_SUFFIX);
    }

    /**
     * Verifies that the header's key is equal to the expected key.
     *
     * @param expected the expected key
     * @return this assertion
     */
    public HeaderAssert hasKeyEqualTo(String expected) {
        isNotNull().key().isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the header's value, decoded as a UTF-8 string, is equal to the expected value.
     *
     * @param expected the expected value, may be null
     * @return this assertion
     */
    public HeaderAssert hasValueEqualTo(String expected) {
        if (expected == null) {
            isNotNull().value().isNull();
        }
        else {
            hasStringValueSatisfying(val -> Assertions.assertThat(val).isEqualTo(expected));
        }
        return this;
    }

    /**
     * Verifies that the header's value is equal to the expected bytes.
     *
     * @param expected the expected value
     * @return this assertion
     */
    public HeaderAssert hasValueEqualTo(byte[] expected) {
        hasByteValueSatisfying(val -> Assertions.assertThat(val).isEqualTo(expected));
        return this;
    }

    /**
     * Verifies that the header's value is null.
     *
     * @return this assertion
     */
    public HeaderAssert hasNullValue() {
        isNotNull().value().isNull();
        return this;
    }

    /**
     * Verifies that the header's value, decoded as a UTF-8 string, satisfies the given assertion.
     *
     * @param assertion the assertion the value must satisfy
     * @return this assertion
     */
    public HeaderAssert hasStringValueSatisfying(ThrowingConsumer<String> assertion) {
        String existingDescription = descriptionText();
        isNotNull().value()
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .asString(StandardCharsets.UTF_8)
                .as(DESCRIBED_AS_PATTERN, existingDescription, VALUE_SUFFIX)
                .satisfies(assertion);

        return this;
    }

    /**
     * Verifies that the header's value satisfies the given assertion.
     *
     * @param assertion the assertion the value must satisfy
     * @return this assertion
     */
    public HeaderAssert hasByteValueSatisfying(ThrowingConsumer<byte[]> assertion) {
        String existingDescription = descriptionText();
        isNotNull().value()
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .as(DESCRIBED_AS_PATTERN, existingDescription, VALUE_SUFFIX)
                .satisfies(assertion);

        return this;
    }

}
