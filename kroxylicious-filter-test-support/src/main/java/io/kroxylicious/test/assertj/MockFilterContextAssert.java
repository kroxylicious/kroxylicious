/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.util.function.Consumer;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.BooleanAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowableAssert;

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.test.context.MockFilterContext.MockErrorRequestFilterResult;

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

    public static ResponseFilterResponseAssert assertThat(ResponseFilterResult actual) {
        return new ResponseFilterResponseAssert(actual);
    }

    public static RequestFilterResponseAssert assertThat(RequestFilterResult actual) {
        return new RequestFilterResponseAssert(actual);
    }

    public static class ResponseFilterResponseAssert extends AbstractAssert<ResponseFilterResponseAssert, ResponseFilterResult> {
        protected ResponseFilterResponseAssert(ResponseFilterResult responseFilterResult) {
            super(responseFilterResult, ResponseFilterResponseAssert.class);
        }

        public ResponseFilterResponseAssert hasHeaderEqualTo(ApiMessage header) {
            isNotNull();
            ObjectAssert<ApiMessage> headerAssert = new ObjectAssert<>(actual.header());
            headerAssert.isEqualTo(header);
            return this;
        }

        public ResponseFilterResponseAssert hasMessageEqualTo(ApiMessage message) {
            isNotNull();
            ObjectAssert<ApiMessage> messageAssert = new ObjectAssert<>(actual.message());
            messageAssert.isEqualTo(message);
            return this;
        }

        public BooleanAssert drop() {
            isNotNull();
            return new BooleanAssert(actual.drop());
        }

        public BooleanAssert closeConnection() {
            isNotNull();
            return new BooleanAssert(actual.closeConnection());
        }

        public ResponseFilterResponseAssert isDropResponse() {
            drop().isTrue();
            return this;
        }

        public ResponseFilterResponseAssert isNotDropResponse() {
            drop().isFalse();
            return this;
        }

        public ResponseFilterResponseAssert isCloseConnection() {
            closeConnection().isTrue();
            return this;
        }

        public ResponseFilterResponseAssert isNotCloseConnection() {
            closeConnection().isFalse();
            return this;
        }
    }

    public static class RequestFilterResponseAssert extends AbstractAssert<RequestFilterResponseAssert, RequestFilterResult> {
        protected RequestFilterResponseAssert(RequestFilterResult requestFilterResult) {
            super(requestFilterResult, RequestFilterResponseAssert.class);
        }

        public RequestFilterResponseAssert hasHeaderEqualTo(ApiMessage header) {
            isNotNull();
            ObjectAssert<ApiMessage> headerAssert = new ObjectAssert<>(actual.header());
            headerAssert.isEqualTo(header);
            return this;
        }

        public RequestFilterResponseAssert hasMessageEqualTo(ApiMessage message) {
            isNotNull();
            ObjectAssert<ApiMessage> messageAssert = new ObjectAssert<>(actual.message());
            messageAssert.isEqualTo(message);
            return this;
        }

        public <T extends ApiMessage> RequestFilterResponseAssert hasMessageInstanceOfSatisfying(Class<T> clazz, Consumer<T> satisfying) {
            isNotNull();
            new ObjectAssert<>(actual.message()).isInstanceOfSatisfying(clazz, satisfying);
            return this;
        }

        public <T extends ApiMessage> RequestFilterResponseAssert hasHeaderInstanceOfSatisfying(Class<T> clazz, Consumer<T> satisfying) {
            isNotNull();
            new ObjectAssert<>(actual.header()).isInstanceOfSatisfying(clazz, satisfying);
            return this;
        }

        public BooleanAssert drop() {
            isNotNull();
            return new BooleanAssert(actual.drop());
        }

        public BooleanAssert closeConnection() {
            isNotNull();
            return new BooleanAssert(actual.closeConnection());
        }

        public BooleanAssert shortCircuit() {
            isNotNull();
            return new BooleanAssert(actual.shortCircuitResponse());
        }

        public RequestFilterResponseAssert isDropRequest() {
            drop().isTrue();
            return this;
        }

        public RequestFilterResponseAssert isNotDropRequest() {
            drop().isFalse();
            return this;
        }

        public RequestFilterResponseAssert isCloseConnection() {
            closeConnection().isTrue();
            return this;
        }

        public RequestFilterResponseAssert isNotCloseConnection() {
            closeConnection().isFalse();
            return this;
        }

        public RequestFilterResponseAssert isShortCircuitResponse() {
            shortCircuit().isTrue();
            return this;
        }

        public RequestFilterResponseAssert isNotShortCircuitResponse() {
            shortCircuit().isFalse();
            return this;
        }

        public ThrowableAssert<ApiException> errorResponse() {
            isNotNull();
            Assertions.assertThat(actual).isInstanceOf(MockErrorRequestFilterResult.class);
            return new ThrowableAssert<>(((MockErrorRequestFilterResult) actual).apiException());
        }

        public RequestFilterResponseAssert isErrorResponse() {
            isNotNull();
            Assertions.assertThat(actual).isInstanceOf(MockErrorRequestFilterResult.class);
            return this;
        }

        public RequestFilterResponseAssert isNotErrorResponse() {
            isNotNull();
            Assertions.assertThat(actual).isNotInstanceOf(MockErrorRequestFilterResult.class);
            return this;
        }

        /**
         * RequestFilterResult is implicitly a forward command if it is not set to short-circuit respond or drop
         */
        public RequestFilterResponseAssert isForwardRequest() {
            isNotNull();
            forwardRequest().isTrue();
            return this;
        }

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
}
