/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PassthroughTest {

    public static Stream<Arguments> illegalConstructions() {
        Arguments.ArgumentSet negativeMinimum = Arguments.argumentSet("negative minimum", (Runnable) () -> createPassthrough(-1, 1));
        Arguments.ArgumentSet tooLargeMinimum = Arguments.argumentSet("too large minimum", (Runnable) () -> createPassthrough(Short.MAX_VALUE + 1, 1));
        Arguments.ArgumentSet negativeMaximum = Arguments.argumentSet("negative maximum", (Runnable) () -> createPassthrough(1, -1));
        Arguments.ArgumentSet tooLargeMaximum = Arguments.argumentSet("too large maximum", (Runnable) () -> createPassthrough(1, Short.MAX_VALUE + 1));
        Arguments.ArgumentSet maxLessThanMin = Arguments.argumentSet("max less than min", (Runnable) () -> createPassthrough(1, 0));
        return Stream.of(negativeMinimum, tooLargeMinimum, negativeMaximum, tooLargeMaximum, maxLessThanMin);
    }

    @ParameterizedTest
    @MethodSource
    void illegalConstructions(Runnable runnable) {
        Assertions.assertThatThrownBy(runnable::run).isInstanceOf(IllegalArgumentException.class);
    }

    public static Stream<Arguments> validVersions() {
        Arguments.ArgumentSet bothMinimum = Arguments.argumentSet("both zero", 0, 0);
        Arguments.ArgumentSet bothMax = Arguments.argumentSet("both maximum possible", (int) Short.MAX_VALUE, (int) Short.MAX_VALUE);
        Arguments.ArgumentSet maxGreaterThanMin = Arguments.argumentSet("max greater than min", 1, 2);
        return Stream.of(bothMinimum, bothMax, maxGreaterThanMin);
    }

    @ParameterizedTest
    @MethodSource("validVersions")
    void minSupportedVersion(int minVersion, int maxVersion) {
        Passthrough<ApiMessage, ApiMessage> passthrough = createPassthrough(minVersion, maxVersion);
        assertThat(passthrough.minSupportedVersion()).isEqualTo((short) minVersion);
        assertThat(passthrough.maxSupportedVersion()).isEqualTo((short) maxVersion);
    }

    @Test
    void requestPassedThrough() {
        int minSupportedVersion = 1;
        int maxSupportedVersion = 1;
        Passthrough<ApiMessage, ApiMessage> passthrough = createPassthrough(minSupportedVersion, maxSupportedVersion);
        RequestHeaderData requestHeaderData = mock(RequestHeaderData.class);
        ApiMessage message = mock(ApiMessage.class);
        FilterContext context = mock(FilterContext.class);
        AuthorizationFilter filter = mock(AuthorizationFilter.class);
        CompletionStage<RequestFilterResult> expectedStage = new CompletableFuture<>();
        Mockito.when(context.forwardRequest(requestHeaderData, message)).thenReturn(expectedStage);
        CompletionStage<RequestFilterResult> actualStage = passthrough.onRequest(requestHeaderData, message, context, filter);
        assertThat(actualStage).isSameAs(expectedStage);
    }

    @Test
    void responsePassedThrough() {
        Passthrough<ApiMessage, ApiMessage> passthrough = createPassthrough(1, 1);
        ResponseHeaderData responseHeaderData = mock(ResponseHeaderData.class);
        ApiMessage message = mock(ApiMessage.class);
        FilterContext context = mock(FilterContext.class);
        AuthorizationFilter filter = mock(AuthorizationFilter.class);
        CompletionStage<ResponseFilterResult> expectedStage = new CompletableFuture<>();
        Mockito.when(context.forwardResponse(responseHeaderData, message)).thenReturn(expectedStage);
        CompletionStage<ResponseFilterResult> actualStage = passthrough.onResponse(responseHeaderData, message, context, filter);
        assertThat(actualStage).isSameAs(expectedStage);
    }

    private static Passthrough<ApiMessage, ApiMessage> createPassthrough(int minSupportedVersion, int maxSupportedVersion) {
        return new Passthrough<>(minSupportedVersion, maxSupportedVersion);
    }
}