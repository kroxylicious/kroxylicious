/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.BooleanAssert;
import org.assertj.core.api.ObjectAssert;

import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.testing.filter.context.MockFilterContext.MockErrorRequestFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Assertions for RequestFilterResult and ResponseFilterResult obtained from {@link MockFilterContextAssert}
 * These are broadly applicable to any implementation, except for asserting a short-circuit error response.
 * This is where the FilterContext is instructed to create an error response for a client. The Mock captures
 * the inputs, but we do not reimplement this error response creation. This allows us to assert that the Filter
 * API was called as expected without having to know how the framework will create the error response.
 */
public class MockFilterContextAssert {

    /**
     * Prevent construction of utility class
     */
    private MockFilterContextAssert() {
        // prevent construction
    }

    /**
     * Creates an assertion for the given ResponseFilterResult.
     *
     * @param actual the actual ResponseFilterResult
     * @return the assertion
     */
    public static ResponseFilterResponseAssert assertThat(ResponseFilterResult actual) {
        return new ResponseFilterResponseAssert(actual);
    }

    /**
     * Creates an assertion for the given RequestFilterResult.
     *
     * @param actual the actual RequestFilterResult
     * @return the assertion
     */
    public static RequestFilterResponseAssert assertThat(RequestFilterResult actual) {
        return new RequestFilterResponseAssert(actual);
    }

    /**
     * AssertJ assertions for {@link ResponseFilterResult}.
     */
    public static class ResponseFilterResponseAssert extends AbstractAssert<ResponseFilterResponseAssert, ResponseFilterResult> {
        /**
         * Constructs an assertion for the given ResponseFilterResult.
         *
         * @param responseFilterResult the actual ResponseFilterResult
         */
        protected ResponseFilterResponseAssert(ResponseFilterResult responseFilterResult) {
            super(responseFilterResult, ResponseFilterResponseAssert.class);
        }

        /**
         * Verifies that the result's header is equal to the expected header.
         *
         * @param header the expected header
         * @return this assertion
         */
        public ResponseFilterResponseAssert hasHeaderEqualTo(ApiMessage header) {
            isNotNull();
            ObjectAssert<ApiMessage> headerAssert = new ObjectAssert<>(actual.header());
            headerAssert.isEqualTo(header);
            return this;
        }

        /**
         * Verifies that the result's message is equal to the expected message.
         *
         * @param message the expected message
         * @return this assertion
         */
        public ResponseFilterResponseAssert hasMessageEqualTo(ApiMessage message) {
            isNotNull();
            ObjectAssert<ApiMessage> messageAssert = new ObjectAssert<>(actual.message());
            messageAssert.isEqualTo(message);
            return this;
        }

        /**
         * Verifies that the result's message is an instance of the given type and satisfies the given requirements.
         *
         * @param <T> the expected type of the message
         * @param clazz the expected type of the message
         * @param satisfying the requirements the message must satisfy
         * @return this assertion
         */
        public <T extends ApiMessage> ResponseFilterResponseAssert hasMessageInstanceOfSatisfying(Class<T> clazz, Consumer<T> satisfying) {
            isNotNull();
            new ObjectAssert<>(actual.message()).isInstanceOfSatisfying(clazz, satisfying);
            return this;
        }

        /**
         * Verifies that the result's header is an instance of the given type and satisfies the given requirements.
         *
         * @param <T> the expected type of the header
         * @param clazz the expected type of the header
         * @param satisfying the requirements the header must satisfy
         * @return this assertion
         */
        public <T extends ApiMessage> ResponseFilterResponseAssert hasHeaderInstanceOfSatisfying(Class<T> clazz, Consumer<T> satisfying) {
            isNotNull();
            new ObjectAssert<>(actual.header()).isInstanceOfSatisfying(clazz, satisfying);
            return this;
        }

        /**
         * Creates an assertion for the result's drop flag.
         *
         * @return the drop flag assertion
         */
        public BooleanAssert drop() {
            isNotNull();
            return new BooleanAssert(actual.drop());
        }

        /**
         * Creates an assertion for the result's close-connection flag.
         *
         * @return the close-connection flag assertion
         */
        public BooleanAssert closeConnection() {
            isNotNull();
            return new BooleanAssert(actual.closeConnection());
        }

        /**
         * Verifies that the result commands the framework to drop the response.
         *
         * @return this assertion
         */
        public ResponseFilterResponseAssert isDropResponse() {
            drop().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to drop the response.
         *
         * @return this assertion
         */
        public ResponseFilterResponseAssert isNotDropResponse() {
            drop().isFalse();
            return this;
        }

        /**
         * Verifies that the result commands the framework to close the connection.
         *
         * @return this assertion
         */
        public ResponseFilterResponseAssert isCloseConnection() {
            closeConnection().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to close the connection.
         *
         * @return this assertion
         */
        public ResponseFilterResponseAssert isNotCloseConnection() {
            closeConnection().isFalse();
            return this;
        }

        /**
         * Verifies that the result commands the framework to forward the response.
         *
         * @return this assertion
         */
        public ResponseFilterResponseAssert isForwardResponse() {
            isNotNull();
            forwardResponse().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to forward the response.
         *
         * @return this assertion
         */
        public ResponseFilterResponseAssert isNotForwardResponse() {
            isNotNull();
            forwardResponse().isFalse();
            return this;
        }

        // the Filter is implicitly commanding the framework to forward the response if it is not set to drop
        @NonNull
        private BooleanAssert forwardResponse() {
            return new BooleanAssert(!actual.drop());
        }
    }

    /**
     * AssertJ assertions for {@link RequestFilterResult}.
     */
    public static class RequestFilterResponseAssert extends AbstractAssert<RequestFilterResponseAssert, RequestFilterResult> {
        /**
         * Constructs an assertion for the given RequestFilterResult.
         *
         * @param requestFilterResult the actual RequestFilterResult
         */
        protected RequestFilterResponseAssert(RequestFilterResult requestFilterResult) {
            super(requestFilterResult, RequestFilterResponseAssert.class);
        }

        /**
         * Verifies that the result's header is equal to the expected header.
         *
         * @param header the expected header
         * @return this assertion
         */
        public RequestFilterResponseAssert hasHeaderEqualTo(ApiMessage header) {
            isNotNull();
            ObjectAssert<ApiMessage> headerAssert = new ObjectAssert<>(actual.header());
            headerAssert.isEqualTo(header);
            return this;
        }

        /**
         * Verifies that the result's message is equal to the expected message.
         *
         * @param message the expected message
         * @return this assertion
         */
        public RequestFilterResponseAssert hasMessageEqualTo(ApiMessage message) {
            isNotNull();
            ObjectAssert<ApiMessage> messageAssert = new ObjectAssert<>(actual.message());
            messageAssert.isEqualTo(message);
            return this;
        }

        /**
         * Verifies that the result's message is an instance of the given type and satisfies the given requirements.
         *
         * @param <T> the expected type of the message
         * @param clazz the expected type of the message
         * @param satisfying the requirements the message must satisfy
         * @return this assertion
         */
        public <T extends ApiMessage> RequestFilterResponseAssert hasMessageInstanceOfSatisfying(Class<T> clazz, Consumer<T> satisfying) {
            isNotNull();
            new ObjectAssert<>(actual.message()).isInstanceOfSatisfying(clazz, satisfying);
            return this;
        }

        /**
         * Verifies that the result's header is an instance of the given type and satisfies the given requirements.
         *
         * @param <T> the expected type of the header
         * @param clazz the expected type of the header
         * @param satisfying the requirements the header must satisfy
         * @return this assertion
         */
        public <T extends ApiMessage> RequestFilterResponseAssert hasHeaderInstanceOfSatisfying(Class<T> clazz, Consumer<T> satisfying) {
            isNotNull();
            new ObjectAssert<>(actual.header()).isInstanceOfSatisfying(clazz, satisfying);
            return this;
        }

        /**
         * Creates an assertion for the result's drop flag.
         *
         * @return the drop flag assertion
         */
        public BooleanAssert drop() {
            isNotNull();
            return new BooleanAssert(actual.drop());
        }

        /**
         * Creates an assertion for the result's close-connection flag.
         *
         * @return the close-connection flag assertion
         */
        public BooleanAssert closeConnection() {
            isNotNull();
            return new BooleanAssert(actual.closeConnection());
        }

        /**
         * Creates an assertion for the result's short-circuit flag.
         *
         * @return the short-circuit flag assertion
         */
        public BooleanAssert shortCircuit() {
            isNotNull();
            return new BooleanAssert(actual.shortCircuitResponse());
        }

        /**
         * Verifies that the result commands the framework to drop the request.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isDropRequest() {
            drop().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to drop the request.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isNotDropRequest() {
            drop().isFalse();
            return this;
        }

        /**
         * Verifies that the result commands the framework to close the connection.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isCloseConnection() {
            closeConnection().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to close the connection.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isNotCloseConnection() {
            closeConnection().isFalse();
            return this;
        }

        /**
         * Verifies that the result commands the framework to send a short-circuit response.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isShortCircuitResponse() {
            shortCircuit().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to send a short-circuit response.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isNotShortCircuitResponse() {
            shortCircuit().isFalse();
            return this;
        }

        /**
         * Verifies that the result is an error response and creates an assertion for its error code and message.
         *
         * @return the error response assertion
         */
        public MockErrorResponseAssert errorResponse() {
            isNotNull();
            Assertions.assertThat(actual).isInstanceOf(MockErrorRequestFilterResult.class);
            return new MockErrorResponseAssert((MockErrorRequestFilterResult) actual);
        }

        /**
         * Verifies that the result commands the framework to send an error response.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isErrorResponse() {
            isNotNull();
            Assertions.assertThat(actual).isInstanceOf(MockErrorRequestFilterResult.class);
            return this;
        }

        /**
         * Verifies that the result does not command the framework to send an error response.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isNotErrorResponse() {
            isNotNull();
            Assertions.assertThat(actual).isNotInstanceOf(MockErrorRequestFilterResult.class);
            return this;
        }

        /**
         * RequestFilterResult is implicitly a forward command if it is not set to short-circuit respond or drop
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isForwardRequest() {
            isNotNull();
            forwardRequest().isTrue();
            return this;
        }

        /**
         * Verifies that the result does not command the framework to forward the request.
         *
         * @return this assertion
         */
        public RequestFilterResponseAssert isNotForwardRequest() {
            isNotNull();
            forwardRequest().isFalse();
            return this;
        }

        @NonNull
        private BooleanAssert forwardRequest() {
            return new BooleanAssert(!actual.shortCircuitResponse() && !actual.drop());
        }
    }

    /**
     * Assertions for the error code and message captured by a short-circuit error response.
     */
    public static class MockErrorResponseAssert {

        private final MockErrorRequestFilterResult actual;

        private MockErrorResponseAssert(MockErrorRequestFilterResult actual) {
            this.actual = actual;
        }

        /**
         * Verifies that the error response was created with the given error code.
         *
         * @param expected the expected error code
         * @return this assertion
         */
        public MockErrorResponseAssert hasError(Errors expected) {
            Assertions.assertThat(actual.error()).isEqualTo(expected);
            return this;
        }

        /**
         * Verifies that the error response has the given message, resolving to the error's default message
         * when no explicit message was supplied.
         *
         * @param expected the expected message
         * @return this assertion
         */
        public MockErrorResponseAssert hasMessage(String expected) {
            String effectiveMessage = actual.errorMessage() != null ? actual.errorMessage() : actual.error().message();
            Assertions.assertThat(effectiveMessage).isEqualTo(expected);
            return this;
        }
    }
}
