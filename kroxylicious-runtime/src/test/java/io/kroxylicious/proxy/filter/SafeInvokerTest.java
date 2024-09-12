/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SafeInvokerTest {

    private final FilterInvoker invoker = Mockito.mock(FilterInvoker.class);
    private final SafeInvoker safeInvoker = new SafeInvoker(invoker);

    @Test
    void testShouldHandleRequestDelegated() {
        // given
        when(invoker.shouldHandleRequest(API_VERSIONS, (short) 1)).thenReturn(true);

        // when
        boolean shouldHandleRequest = safeInvoker.shouldHandleRequest(API_VERSIONS, (short) 1);

        // then
        assertThat(shouldHandleRequest).isTrue();
        verify(invoker).shouldHandleRequest(API_VERSIONS, (short) 1);
    }

    @Test
    void testOnRequestExceptionsInShouldHandleIsConvertedToFuture() {
        // given
        RuntimeException exception = new RuntimeException("boom!");
        when(invoker.shouldHandleRequest(API_VERSIONS, (short) 1)).thenThrow(exception);
        ApiVersionsRequestData message = new ApiVersionsRequestData();
        RequestHeaderData header = new RequestHeaderData();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        CompletableFuture<RequestFilterResult> forwardStage = new CompletableFuture<>();
        when(filterContext.forwardRequest(header, message)).thenReturn(forwardStage);

        // when
        CompletionStage<RequestFilterResult> stage = safeInvoker.onRequest(API_VERSIONS, (short) 1, header, message, filterContext);

        // then
        assertThat(stage).failsWithin(Duration.ZERO)
                         .withThrowableThat()
                         .isInstanceOf(ExecutionException.class)
                         .withCause(exception);
        verify(invoker).shouldHandleRequest(API_VERSIONS, (short) 1);
    }

    @Test
    void testOnRequestExceptionsInOnRequestIsConvertedToFuture() {
        // given
        RuntimeException exception = new RuntimeException("boom!");
        when(invoker.shouldHandleRequest(API_VERSIONS, (short) 1)).thenReturn(true);
        ApiVersionsRequestData message = new ApiVersionsRequestData();
        RequestHeaderData header = new RequestHeaderData();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        CompletableFuture<RequestFilterResult> forwardStage = new CompletableFuture<>();
        when(filterContext.forwardRequest(header, message)).thenReturn(forwardStage);
        when(invoker.onRequest(API_VERSIONS, (short) 1, header, message, filterContext)).thenThrow(exception);

        // when
        CompletionStage<RequestFilterResult> stage = safeInvoker.onRequest(API_VERSIONS, (short) 1, header, message, filterContext);

        // then
        assertThat(stage).failsWithin(Duration.ZERO)
                         .withThrowableThat()
                         .isInstanceOf(ExecutionException.class)
                         .withCause(exception);
    }

    @Test
    void testShouldHandleResponseDelegated() {
        // given
        when(invoker.shouldHandleResponse(API_VERSIONS, (short) 1)).thenReturn(true);

        // when
        boolean shouldHandleResponse = safeInvoker.shouldHandleResponse(API_VERSIONS, (short) 1);

        // then
        assertThat(shouldHandleResponse).isTrue();
        verify(invoker).shouldHandleResponse(API_VERSIONS, (short) 1);
    }

    @Test
    void testIfDelegateNotInterestedInRequestSafeInvokerForwards() {
        // given
        when(invoker.shouldHandleRequest(API_VERSIONS, (short) 1)).thenReturn(false);
        ApiVersionsRequestData message = new ApiVersionsRequestData();
        RequestHeaderData header = new RequestHeaderData();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        CompletableFuture<RequestFilterResult> forwardStage = new CompletableFuture<>();
        when(filterContext.forwardRequest(header, message)).thenReturn(forwardStage);

        // when
        CompletionStage<RequestFilterResult> stage = safeInvoker.onRequest(
                API_VERSIONS,
                (short) 1,
                header,
                message,
                filterContext
        );

        // then
        assertThat(stage).isSameAs(forwardStage);
    }

    @Test
    void testIfDelegateInterestedInRequestSafeInvokerDelegates() {
        // given
        when(invoker.shouldHandleRequest(API_VERSIONS, (short) 1)).thenReturn(true);
        ApiVersionsRequestData message = new ApiVersionsRequestData();
        RequestHeaderData header = new RequestHeaderData();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        CompletableFuture<RequestFilterResult> delegateStage = new CompletableFuture<>();
        when(invoker.onRequest(any(), anyShort(), any(), any(), any())).thenReturn(delegateStage);

        // when
        CompletionStage<RequestFilterResult> stage = safeInvoker.onRequest(
                API_VERSIONS,
                (short) 1,
                header,
                message,
                filterContext
        );

        // then
        assertThat(stage).isSameAs(delegateStage);
        verify(invoker).onRequest(API_VERSIONS, (short) 1, header, message, filterContext);
    }

    @Test
    void testIfDelegateInterestedInResponseSafeInvokerDelegates() {
        // given
        when(invoker.shouldHandleResponse(API_VERSIONS, (short) 1)).thenReturn(true);
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        ResponseHeaderData header = new ResponseHeaderData();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        CompletableFuture<ResponseFilterResult> delegateStage = new CompletableFuture<>();
        when(invoker.onResponse(any(), anyShort(), any(), any(), any())).thenReturn(delegateStage);

        // when
        CompletionStage<ResponseFilterResult> stage = safeInvoker.onResponse(
                API_VERSIONS,
                (short) 1,
                header,
                message,
                filterContext
        );

        // then
        assertThat(stage).isSameAs(delegateStage);
        verify(invoker).onResponse(API_VERSIONS, (short) 1, header, message, filterContext);
    }

    @Test
    void testIfDelegateNotInterestedInResponseSafeInvokerForwards() {
        // given
        when(invoker.shouldHandleResponse(API_VERSIONS, (short) 1)).thenReturn(false);
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        ResponseHeaderData header = new ResponseHeaderData();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        CompletableFuture<ResponseFilterResult> forwardStage = new CompletableFuture<>();
        when(filterContext.forwardResponse(header, message)).thenReturn(forwardStage);

        // when
        CompletionStage<ResponseFilterResult> stage = safeInvoker.onResponse(
                API_VERSIONS,
                (short) 1,
                header,
                message,
                filterContext
        );

        // then
        assertThat(stage).isSameAs(forwardStage);
    }

    @Test
    void testIfDelegateReturnsNullOnRequestThenFailedFutureReturned() {
        // given
        when(invoker.shouldHandleRequest(API_VERSIONS, (short) 1)).thenReturn(true);
        when(invoker.onRequest(any(), anyShort(), any(), any(), any())).thenReturn(null);

        // when
        CompletionStage<RequestFilterResult> stage = safeInvoker.onRequest(
                API_VERSIONS,
                (short) 1,
                new RequestHeaderData(),
                new ApiVersionsRequestData(),
                Mockito.mock(FilterContext.class)
        );

        // then
        assertThat(stage).isCompletedExceptionally()
                         .failsWithin(Duration.ZERO)
                         .withThrowableOfType(ExecutionException.class)
                         .withCauseInstanceOf(IllegalStateException.class)
                         .havingCause()
                         .withMessageContaining("invoker onRequest returned null for apiKey API_VERSIONS");
    }

    @Test
    void testIfDelegateReturnsNullOnResponseThenFailedFutureReturned() {
        // given
        when(invoker.shouldHandleResponse(API_VERSIONS, (short) 1)).thenReturn(true);
        when(invoker.onResponse(any(), anyShort(), any(), any(), any())).thenReturn(null);

        // when
        CompletionStage<ResponseFilterResult> stage = safeInvoker.onResponse(
                API_VERSIONS,
                (short) 1,
                new ResponseHeaderData(),
                new ApiVersionsResponseData(),
                Mockito.mock(FilterContext.class)
        );

        // then
        assertThat(stage).isCompletedExceptionally()
                         .failsWithin(Duration.ZERO)
                         .withThrowableOfType(ExecutionException.class)
                         .withCauseInstanceOf(IllegalStateException.class)
                         .havingCause()
                         .withMessageContaining("invoker onResponse returned null for apiKey API_VERSIONS");
    }

    @Test
    void testOnResponseIfDelegateShouldHandleThrowsFailedFutureReturned() {
        // given
        RuntimeException exception = new RuntimeException("boom!");
        when(invoker.shouldHandleResponse(API_VERSIONS, (short) 1)).thenThrow(exception);

        // when
        CompletionStage<ResponseFilterResult> stage = safeInvoker.onResponse(
                API_VERSIONS,
                (short) 1,
                new ResponseHeaderData(),
                new ApiVersionsResponseData(),
                Mockito.mock(FilterContext.class)
        );

        // then
        assertThat(stage).isCompletedExceptionally()
                         .failsWithin(Duration.ZERO)
                         .withThrowableOfType(ExecutionException.class)
                         .withCause(exception);
    }

    @Test
    void testOnResponseIfDelegateOnResponseThrowsFailedFutureReturned() {
        // given
        RuntimeException exception = new RuntimeException("boom!");
        when(invoker.shouldHandleResponse(API_VERSIONS, (short) 1)).thenReturn(true);
        when(invoker.onResponse(any(), anyShort(), any(), any(), any())).thenThrow(exception);

        // when
        CompletionStage<ResponseFilterResult> stage = safeInvoker.onResponse(
                API_VERSIONS,
                (short) 1,
                new ResponseHeaderData(),
                new ApiVersionsResponseData(),
                Mockito.mock(FilterContext.class)
        );

        // then
        assertThat(stage).isCompletedExceptionally()
                         .failsWithin(Duration.ZERO)
                         .withThrowableOfType(ExecutionException.class)
                         .withCause(exception);
    }

}
