/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;

public interface Correlatable<S extends Referenceable<S> & Loggable> {
    interface CorrelationId<T> {}
    CorrelationId<Integer> CLIENT_CORRELATION_ID = new CorrelationId<Integer>() {
    };
    CorrelationId<Integer> SERVER_CORRELATION_ID = new CorrelationId<Integer>() {
    };
    CorrelationId<String> SPAN_ID = new CorrelationId<String>() {
    };
    CorrelationId<String> TRACE_ID = new CorrelationId<String>() {
    };
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    <T> S addCorrelation(CorrelationId<T> correlationKey, T correlationId);
}
