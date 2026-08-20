/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.kafka.common.requests.AbstractResponse;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.api.MapAssert;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.protocol.MessageUtil;

/**
 * AssertJ assertions for a vendored {@link ApiMessage} representing a response.
 *
 * @param <T> the type of response the assertion applies to
 */
public class ResponseAssert<T extends ApiMessage> extends AbstractAssert<ResponseAssert<T>, T> {

    private final short apiVersion;

    /**
     * Constructs an assertion for the given response.
     *
     * @param t the actual response
     * @param apiVersion the API version at which the response was encoded
     */
    protected ResponseAssert(T t, short apiVersion) {
        super(t, ResponseAssert.class);
        this.apiVersion = apiVersion;
    }

    /**
     * Creates an assertion for the given response.
     *
     * @param <X> the type of response the assertion applies to
     * @param actual the actual response
     * @param apiVersion the API version at which the response was encoded
     * @return the assertion
     */
    public static <X extends ApiMessage> ResponseAssert<X> assertThat(X actual, short apiVersion) {
        return new ResponseAssert<>(actual, apiVersion);
    }

    /**
     * Verifies that the response has the expected API key.
     *
     * @param apiKey the expected API key
     * @return this assertion
     */
    public ResponseAssert<T> hasApiKey(ApiKeys apiKey) {
        if (!Objects.equals(actual.apiKey(), apiKey.id)) {
            failWithMessage("Expected message with apiKey <%s> but was <%s>", apiKey, ApiKeys.forId(actual.apiKey()));
        }
        return this;
    }

    /**
     * Verifies that the response has at least the given number of errors of the given type.
     * <p>
     * The vendored response message carries no generic error-counting API (unlike kafka-clients'
     * {@code AbstractResponse}), so the response is round-tripped through kafka-clients to reuse
     * its per-response {@code errorCounts()} implementation.
     *
     * @param errorType the expected error type
     * @param errorCount the minimum expected number of errors of that type
     * @return this assertion
     */
    public ResponseAssert<T> hasErrorCount(Errors errorType, int errorCount) {
        isNotNull();
        AbstractResponse kafkaResponse = toKafkaClientsResponse();
        org.apache.kafka.common.protocol.Errors kafkaErrorType = org.apache.kafka.common.protocol.Errors.forCode(errorType.code());
        MapAssert.assertThatMap(kafkaResponse.errorCounts())
                .as("Expected response to have errors")
                .isNotEmpty()
                .hasEntrySatisfying(kafkaErrorType,
                        new Condition<>() {
                            @Override
                            public boolean matches(Integer value) {
                                return value >= errorCount;
                            }
                        });

        return this;
    }

    private AbstractResponse toKafkaClientsResponse() {
        ApiKeys vendoredApiKey = ApiKeys.forId(actual.apiKey());
        var kafkaApiKey = org.apache.kafka.common.protocol.ApiKeys.forId(vendoredApiKey.id);
        ByteBuffer bytes = MessageUtil.toByteBufferAccessor(actual, apiVersion).buffer();
        return AbstractResponse.parseResponse(kafkaApiKey, new org.apache.kafka.common.protocol.ByteBufferAccessor(bytes), apiVersion);
    }

}
