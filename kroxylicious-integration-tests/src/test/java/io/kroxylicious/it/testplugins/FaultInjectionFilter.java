/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * A test-only filter that can simulate network faults on demand.
 * Starts in pass-through mode; tests activate faults via {@link Handle}.
 */
class FaultInjectionFilter implements RequestFilter, ResponseFilter {

    /**
     * The type of fault to inject.
     */
    enum FaultMode {
        /** No fault — pass through normally. */
        NONE,
        /** Close the connection immediately. */
        CLOSE,
        /** Drop the message silently (blackhole). */
        DROP
    }

    /**
     * Test-facing control for activating and deactivating faults.
     */
    public static final class Handle {

        private final AtomicReference<FaultMode> mode;

        Handle(AtomicReference<FaultMode> mode) {
            this.mode = mode;
        }

        /** Activate close-connection fault mode. */
        public void closeOnNextRequest() {
            mode.set(FaultMode.CLOSE);
        }

        /** Activate blackhole fault mode (silently drop messages). */
        public void dropAll() {
            mode.set(FaultMode.DROP);
        }

        /** Return to normal pass-through mode. */
        public void passThrough() {
            mode.set(FaultMode.NONE);
        }

        public FaultMode currentMode() {
            return mode.get();
        }
    }

    private final AtomicReference<FaultMode> mode = new AtomicReference<>(FaultMode.NONE);

    Handle handle() {
        return new Handle(mode);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        return switch (mode.get()) {
            case NONE -> context.forwardRequest(header, request);
            case CLOSE -> context.requestFilterResultBuilder()
                    .withCloseConnection()
                    .completed();
            case DROP -> context.requestFilterResultBuilder()
                    .drop()
                    .completed();
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        return switch (mode.get()) {
            case NONE -> context.forwardResponse(header, response);
            case CLOSE -> context.responseFilterResultBuilder()
                    .withCloseConnection()
                    .completed();
            case DROP -> context.responseFilterResultBuilder()
                    .drop()
                    .completed();
        };
    }
}
