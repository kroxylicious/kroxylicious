/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

public enum ForwardingStyle implements BiFunction<KrpcFilterContext, ApiMessage, CompletionStage<ApiMessage>> {
    SYNCHRONOUS {
        @Override
        public CompletionStage<ApiMessage> apply(KrpcFilterContext context, ApiMessage body) {
            return CompletableFuture.completedStage(body);
        }
    },
    ASYNCHRONOUS_DELAYED {
        @Override
        public CompletionStage<ApiMessage> apply(KrpcFilterContext context, ApiMessage body) {
            CompletableFuture<ApiMessage> result = new CompletableFuture<>();
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            try {
                executor.schedule(() -> {
                    result.complete(body);
                }, 200L, TimeUnit.MILLISECONDS);
            }
            finally {
                executor.shutdown();
            }
            return result;
        }
    },
    ASYNCHRONOUS_REQUEST_TO_BROKER {
        @Override
        public CompletionStage<ApiMessage> apply(KrpcFilterContext context, ApiMessage body) {
            return sendAsyncRequestAndCheckForResponseErrors(context).thenApply(unused -> body);
        }

        private CompletionStage<ListGroupsResponseData> sendAsyncRequestAndCheckForResponseErrors(KrpcFilterContext filterContext) {
            return filterContext.<ListGroupsResponseData> sendRequest(ApiKeys.LIST_GROUPS.latestVersion(), new ListGroupsRequestData())
                    .thenApply(r -> {
                        if (r.errorCode() != Errors.NONE.code()) {
                            throw new RuntimeException("Async request unexpected failed (errorCode: %d)".formatted(r.errorCode()));
                        }
                        return r;
                    });
        }
    }
}
