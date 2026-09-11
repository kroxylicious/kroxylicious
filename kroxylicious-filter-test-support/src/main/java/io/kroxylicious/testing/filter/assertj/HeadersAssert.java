/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.header.Headers;

/**
 * AssertJ assertions for {@link Headers}.
 */
public class HeadersAssert extends AbstractAssert<HeadersAssert, Headers> {
    /**
     * Constructs an assertion for the given Headers.
     *
     * @param headers the actual Headers
     */
    protected HeadersAssert(Headers headers) {
        super(headers, HeadersAssert.class);
        describedAs(headers == null ? "null headers" : "headers");
    }

    /**
     * Creates an assertion for the given Headers.
     *
     * @param actual the actual Headers
     * @return the assertion
     */
    public static HeadersAssert assertThat(Headers actual) {
        return new HeadersAssert(actual);
    }

    private IterableAssert<Header> headerIterable() {
        isNotNull();
        return IterableAssert.assertThatIterable(actual)
                .describedAs("headers");
    }

    /**
     * Verifies that the headers are not empty and creates an assertion for the first header.
     *
     * @return the first header assertion
     */
    public HeaderAssert firstHeader() {
        isNotNull();
        return headerIterable()
                .isNotEmpty()
                .first(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("first header");
    }

    /**
     * Creates an assertion for the last header.
     *
     * @return the last header assertion
     */
    public HeaderAssert lastHeader() {
        isNotNull();
        return headerIterable()
                .last(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("last header");
    }

    /**
     * Verifies that there is exactly one header and creates an assertion for it.
     *
     * @return the single header assertion
     */
    public HeaderAssert singleHeader() {
        isNotNull();
        return headerIterable()
                .singleElement(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("single header");
    }

    /**
     * Verifies that there is at least one header with the given key and creates an assertion for the first of them.
     *
     * @param key the header key
     * @return the first matching header assertion
     */
    public HeaderAssert firstHeaderWithKey(String key) {
        isNotNull();
        return extracting(actual -> actual.headers(key), InstanceOfAssertFactories.iterable(Header.class))
                .as("headers with key " + key)
                .isNotEmpty()
                .first(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("first header with key " + key);
    }

    /**
     * Verifies that there is at least one header with the given key and creates an assertion for the last of them.
     *
     * @param key the header key
     * @return the last matching header assertion
     */
    public HeaderAssert lastHeaderWithKey(String key) {
        isNotNull();
        return extracting(actual -> actual.headers(key), InstanceOfAssertFactories.iterable(Header.class))
                .as("headers with key " + key)
                .isNotEmpty()
                .last(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("last header with key " + key);
    }

    /**
     * Verifies that there is exactly one header with the given key and creates an assertion for it.
     *
     * @param key the header key
     * @return the single matching header assertion
     */
    public HeaderAssert singleHeaderWithKey(String key) {
        isNotNull();
        return extracting(actual -> actual.headers(key), InstanceOfAssertFactories.iterable(Header.class))
                .as("headers with key " + key)
                .isNotEmpty()
                .singleElement(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("single header with key " + key);
    }

    /**
     * Verifies that the headers have the expected size.
     *
     * @param expected the expected number of headers
     * @return this assertion
     */
    public HeadersAssert hasSize(int expected) {
        headerIterable()
                .hasSize(expected);
        return this;
    }
}
