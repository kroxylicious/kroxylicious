/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.TerminalStage;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Package-private sealed implementation of {@link RouterResponse}.
 *
 * <p>Instances are created by {@link RouterContextImpl} via the builder methods
 * ({@code respondWith}, {@code respondWithError}, {@code respondWithoutReply}).
 * {@link io.kroxylicious.proxy.internal.routing.RouterDispatchHandler} switches on
 * the subtypes to determine how to deliver the result to the client.</p>
 */
sealed interface RouterResponseImpl extends RouterResponse
        permits RouterResponseImpl.RespondWith,
        RouterResponseImpl.RespondWithError,
        RouterResponseImpl.RespondWithoutReply {

    boolean closeConnection();

    /**
     * Deliver a response body to the client. If {@code header} is null the runtime
     * supplies a default header; otherwise the provided header is used.
     */
    record RespondWith(
                       @Nullable ResponseHeaderData header,
                       ApiMessage body,
                       boolean closeConnection)
            implements RouterResponseImpl {}

    /**
     * Generate an API-specific error response for the client.
     */
    record RespondWithError(
                            RequestHeaderData requestHeader,
                            ApiMessage request,
                            ApiException exception,
                            boolean closeConnection)
            implements RouterResponseImpl {}

    /**
     * No response is delivered to the client (e.g. acks=0 PRODUCE).
     */
    record RespondWithoutReply(boolean closeConnection) implements RouterResponseImpl {}

    /**
     * Mutable builder that implements {@link CloseOrTerminalStage} so router
     * implementations can chain {@code .withCloseConnection().build()} or just
     * call {@code .build()} / {@code .completed()}.
     */
    final class Builder implements CloseOrTerminalStage {

        private final RouterResponseImpl prototype;
        private boolean close;

        Builder(RouterResponseImpl prototype) {
            this.prototype = prototype;
        }

        @Override
        public TerminalStage withCloseConnection() {
            this.close = true;
            return this;
        }

        @Override
        public RouterResponse build() {
            if (!close) {
                return prototype;
            }
            return switch (prototype) {
                case RespondWith rw -> new RespondWith(rw.header(), rw.body(), true);
                case RespondWithError rwe -> new RespondWithError(rwe.requestHeader(), rwe.request(), rwe.exception(), true);
                case RespondWithoutReply ignored -> new RespondWithoutReply(true);
            };
        }

        @Override
        public CompletionStage<RouterResponse> completed() {
            return CompletableFuture.completedFuture(build());
        }
    }

    static CloseOrTerminalStage builder(RouterResponseImpl prototype) {
        return new Builder(prototype);
    }
}
