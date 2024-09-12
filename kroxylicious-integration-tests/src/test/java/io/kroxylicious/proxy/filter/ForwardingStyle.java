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
import java.util.function.Function;

import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

public enum ForwardingStyle implements Function<ForwardingContext, CompletionStage<ApiMessage>> {
    SYNCHRONOUS {
        @Override
        public CompletionStage<ApiMessage> apply(ForwardingContext context) {
            return CompletableFuture.completedStage(context.body());
        }
    },
    ASYNCHRONOUS_DELAYED {
        @Override
        public CompletionStage<ApiMessage> apply(ForwardingContext context) {
            CompletableFuture<ApiMessage> result = new CompletableFuture<>();
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            try {
                executor.schedule(() -> {
                    result.complete(context.body());
                }, 200L, TimeUnit.MILLISECONDS);
            }
            finally {
                executor.shutdown();
            }
            return result;
        }
    },
    ASYNCHRONOUS_DELAYED_ON_EVENTlOOP {
        @Override
        public CompletionStage<ApiMessage> apply(ForwardingContext context) {
            ScheduledExecutorService executor = context.constructionContext().filterDispatchExecutor();
            CompletableFuture<ApiMessage> result = new CompletableFuture<>();
            executor.schedule(() -> {
                result.complete(context.body());
            }, 200L, TimeUnit.MILLISECONDS);
            return result;
        }
    },
    ASYNCHRONOUS_REQUEST_TO_BROKER {
        @Override
        public CompletionStage<ApiMessage> apply(ForwardingContext context) {
            return sendAsyncRequestAndCheckForResponseErrors(context.filterContext()).thenApply(unused -> context.body());
        }

        private CompletionStage<ListGroupsResponseData> sendAsyncRequestAndCheckForResponseErrors(FilterContext filterContext) {
            var request = new ListGroupsRequestData();
            return filterContext.<ListGroupsResponseData> sendRequest(new RequestHeaderData().setRequestApiVersion(request.highestSupportedVersion()), request)
                                .thenApply(r -> {
                                    if (r.errorCode() != Errors.NONE.code()) {
                                        throw new RuntimeException("Async request unexpected failed (errorCode: %d)".formatted(r.errorCode()));
                                    }
                                    return r;
                                });
        }
    }
}
