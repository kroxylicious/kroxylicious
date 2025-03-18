/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.management;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UnsupportedHttpMethodFilterTest {

    @Mock
    private HttpExchange httpExchange;

    @Mock
    private Headers responseHeaders;

    @Mock
    private Filter.Chain remainingFilterChain;

    @BeforeEach
    void setUp() {
        lenient().when(httpExchange.getRequestBody()).thenReturn(InputStream.nullInputStream());
    }

    @Test
    void shouldForwardGetRequestToFilterChain() throws IOException {
        // Given
        when(httpExchange.getRequestMethod()).thenReturn("GET");

        // When
        UnsupportedHttpMethodFilter.INSTANCE.doFilter(httpExchange, remainingFilterChain);

        // Then
        verify(remainingFilterChain).doFilter(httpExchange);
    }

    @ParameterizedTest
    @CsvSource({ "TRACE", "OPTIONS", "HEAD", "POST", "PUT", "CONNECT", "PATCH", "DELETE" })
    void shouldRejectRequestViaFilterWithHttpMethod(String httpMethod) throws IOException {
        // Given
        when(httpExchange.getRequestMethod()).thenReturn(httpMethod);
        when(httpExchange.getResponseHeaders()).thenReturn(responseHeaders);

        // When
        UnsupportedHttpMethodFilter.INSTANCE.doFilter(httpExchange, remainingFilterChain);

        // Then
        verify(httpExchange).sendResponseHeaders(405, -1);
        verify(responseHeaders).add("Allow", "GET");
        verify(httpExchange).close();
    }

    @ParameterizedTest
    @CsvSource({ "TRACE", "OPTIONS", "HEAD", "POST", "PUT", "CONNECT", "PATCH", "DELETE" })
    void shouldShorCircuitFilterChainWithHttpMethod(String httpMethod) throws IOException {
        // Given
        when(httpExchange.getRequestMethod()).thenReturn(httpMethod);
        when(httpExchange.getResponseHeaders()).thenReturn(responseHeaders);

        // When
        UnsupportedHttpMethodFilter.INSTANCE.doFilter(httpExchange, remainingFilterChain);

        // Then
        verify(remainingFilterChain, never()).doFilter(any(HttpExchange.class));
    }
}