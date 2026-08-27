/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

/**
 * Builds error {@code *ResponseData} bodies directly from a request {@code *ResponseData}, an {@link ApiKeys}
 * and an {@link Errors} code, without constructing an {@code org.apache.kafka.common.requests.AbstractRequest}
 * and delegating to its {@code getErrorResponse(Throwable)}.
 * <p>
 * Kafka's own per-RPC {@code getErrorResponse} bodies are hand-written, not generated, so this factory ports
 * that logic across explicitly, one {@link ApiKeys} case at a time, rather than depending on kafka-clients'
 * request/response wrapper classes ({@code AbstractRequest}/{@code AbstractResponse}) which are out of scope
 * for the {@code io.kroxylicious.kafka.*} migration (see kroxylicious/kroxylicious#4748).
 * <p>
 * kafka-clients always synthesises error responses with a throttle time of 0 (see
 * {@code AbstractRequest.getErrorResponse(Throwable)} delegating to {@code AbstractResponse.DEFAULT_THROTTLE_TIME}),
 * so this factory does the same rather than taking a throttleTimeMs parameter.
 * <p>
 * Coverage is partial: unhandled {@link ApiKeys} throw {@link UnsupportedOperationException}. Full coverage is
 * tracked incrementally; see the class's test coverage for the current state.
 * <p>
 * {@code null} is a legitimate return value for {@code PRODUCE} when the request's {@code acks} is 0 — the
 * client doesn't want a response at all, mirroring {@code ProduceRequest.getErrorResponse}.
 */
public final class ErrorResponseFactory {

    private ErrorResponseFactory() {
    }

}
