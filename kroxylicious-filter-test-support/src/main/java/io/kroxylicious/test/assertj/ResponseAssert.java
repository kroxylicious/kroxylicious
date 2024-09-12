/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.api.MapAssert;

public class ResponseAssert<T extends AbstractResponse> extends AbstractAssert<ResponseAssert<T>, T> {
    protected ResponseAssert(T t) {
        super(t, ResponseAssert.class);
    }

    public static <X extends AbstractResponse> ResponseAssert<X> assertThat(X actual) {
        return new ResponseAssert<>(actual);
    }

    public ResponseAssert<T> hasApiKey(ApiKeys apiKey) {
        if (!Objects.equals(actual.apiKey(), apiKey)) {
            failWithMessage("Expected message with apiKey <%s> but was <%s>", apiKey, actual.apiKey());
        }
        return this;
    }

    public ResponseAssert<T> hasErrorCount(Errors errorType, int errorCount) {
        isNotNull();
        MapAssert.assertThatMap(this.actual.errorCounts())
                 .as("Expected response to have errors")
                 .isNotEmpty()
                 .hasEntrySatisfying(
                         errorType,
                         new Condition<>() {
                             @Override
                             public boolean matches(Integer value) {
                                 return value >= errorCount;
                             }
                         }
                 );

        return this;
    }

}
