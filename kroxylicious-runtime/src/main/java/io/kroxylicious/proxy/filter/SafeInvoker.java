/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a delegate invoker so that onRequest and onResponse can be safely called even if this
 * Invoker does not want to handle this message, in this case the message will be forwarded without
 * the delegate doing anything with it.
 *
 * @param invoker the delegate
 */
record SafeInvoker(FilterInvoker invoker) implements FilterInvoker {

    private static final Logger logger = LoggerFactory.getLogger(SafeInvoker.class);

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, FilterContext filterContext) {
        try {
            if (invoker.shouldHandleRequest(apiKey, apiVersion)) {
                CompletionStage<RequestFilterResult> stage = invoker.onRequest(apiKey, apiVersion, header, body, filterContext);
                if (stage == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("invoker onRequest returned null for apiKey {}, apiVersion {}, channel: {}," +
                                " Filters should always return a CompletionStage", apiKey, apiVersion, filterContext.sessionId());
                    }
                    return CompletableFuture.failedFuture(new IllegalStateException("invoker onRequest returned null for apiKey " + apiKey));
                }
                return stage;
            }
            else {
                return filterContext.forwardRequest(header, body);
            }
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, FilterContext filterContext) {
        try {
            if (invoker.shouldHandleResponse(apiKey, apiVersion)) {
                CompletionStage<ResponseFilterResult> stage = invoker.onResponse(apiKey, apiVersion, header, body, filterContext);
                if (stage == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("invoker onResponse returned null for apiKey {}, apiVersion {}, channel: {}," +
                                " Filters should always return a CompletionStage", apiKey, apiVersion, filterContext.sessionId());
                    }
                    return CompletableFuture.failedFuture(new IllegalStateException("invoker onResponse returned null for apiKey " + apiKey));
                }
                return stage;
            }
            else {
                return filterContext.forwardResponse(header, body);
            }
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return invoker.shouldHandleRequest(apiKey, apiVersion);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return invoker.shouldHandleResponse(apiKey, apiVersion);
    }

}
