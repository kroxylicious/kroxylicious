/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

public class HeadersAssert extends AbstractAssert<HeadersAssert, Headers> {
    protected HeadersAssert(Headers headers) {
        super(headers, HeadersAssert.class);
        describedAs(headers == null ? "null headers" : "headers");
    }

    public static HeadersAssert assertThat(Headers actual) {
        return new HeadersAssert(actual);
    }

    private IterableAssert<Header> headerIterable() {
        isNotNull();
        IterableAssert<Header> records = IterableAssert.assertThatIterable(actual)
                .describedAs("headers");
        return records;
    }

    public HeaderAssert firstHeader() {
        isNotNull();
        return headerIterable()
                .isNotEmpty()
                .first(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("first header");
    }

    public HeaderAssert lastHeader() {
        isNotNull();
        return headerIterable()
                .last(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("last header");
    }

    public HeaderAssert singleHeader() {
        isNotNull();
        return headerIterable()
                .singleElement(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("single header");
    }

    public HeaderAssert firstHeaderWithKey(String key) {
        isNotNull();
        return extracting(actual -> actual.headers(key), InstanceOfAssertFactories.iterable(Header.class))
                .as("headers with key " + key)
                .isNotEmpty()
                .first(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("first header with key " + key);
    }

    public HeaderAssert lastHeaderWithKey(String key) {
        isNotNull();
        return extracting(actual -> actual.headers(key), InstanceOfAssertFactories.iterable(Header.class))
                .as("headers with key " + key)
                .isNotEmpty()
                .last(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("last header with key " + key);
    }

    public HeaderAssert singleHeaderWithKey(String key) {
        isNotNull();
        return extracting(actual -> actual.headers(key), InstanceOfAssertFactories.iterable(Header.class))
                .as("headers with key " + key)
                .isNotEmpty()
                .singleElement(new InstanceOfAssertFactory<>(Header.class, HeaderAssert::assertThat))
                .describedAs("single header with key " + key);
    }

    public HeadersAssert hasSize(int expected) {
        headerIterable()
                .hasSize(expected);
        return this;
    }
}
