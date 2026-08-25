/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.util.Objects;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.api.MapAssert;

/**
 * AssertJ assertions for {@link AbstractResponse}.
 *
 * @param <T> the type of response the assertion applies to
 */
public class ResponseAssert<T extends AbstractResponse> extends AbstractAssert<ResponseAssert<T>, T> {
    /**
     * Constructs an assertion for the given response.
     *
     * @param t the actual response
     */
    protected ResponseAssert(T t) {
        super(t, ResponseAssert.class);
    }

    /**
     * Creates an assertion for the given response.
     *
     * @param <X> the type of response the assertion applies to
     * @param actual the actual response
     * @return the assertion
     */
    public static <X extends AbstractResponse> ResponseAssert<X> assertThat(X actual) {
        return new ResponseAssert<>(actual);
    }

    /**
     * Verifies that the response has the expected API key.
     *
     * @param apiKey the expected API key
     * @return this assertion
     */
    public ResponseAssert<T> hasApiKey(ApiKeys apiKey) {
        if (!Objects.equals(actual.apiKey(), apiKey)) {
            failWithMessage("Expected message with apiKey <%s> but was <%s>", apiKey, actual.apiKey());
        }
        return this;
    }

    /**
     * Verifies that the response has at least the given number of errors of the given type.
     *
     * @param errorType the expected error type
     * @param errorCount the minimum expected number of errors of that type
     * @return this assertion
     */
    public ResponseAssert<T> hasErrorCount(Errors errorType, int errorCount) {
        isNotNull();
        MapAssert.assertThatMap(this.actual.errorCounts())
                .as("Expected response to have errors")
                .isNotEmpty()
                .hasEntrySatisfying(errorType,
                        new Condition<>() {
                            @Override
                            public boolean matches(Integer value) {
                                return value >= errorCount;
                            }
                        });

        return this;
    }

}
