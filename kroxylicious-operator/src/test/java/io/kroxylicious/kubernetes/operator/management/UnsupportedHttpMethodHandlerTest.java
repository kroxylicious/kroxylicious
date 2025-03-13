/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.management;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UnsupportedHttpMethodHandlerTest {

    private UnsupportedHttpMethodHandler unsupportedHttpMethodHandler;

    @Mock
    private HttpExchange httpExchange;

    @Mock
    private Headers responseHeaders;

    @BeforeEach
    void setUp() {
        unsupportedHttpMethodHandler = new UnsupportedHttpMethodHandler();
    }

    @Test
    void shouldForwardGetRequestToDelegate() throws IOException {
        // Given
        final HttpHandler delegate = mock(HttpHandler.class);
        when(httpExchange.getRequestMethod()).thenReturn("GET");
        unsupportedHttpMethodHandler = new UnsupportedHttpMethodHandler(delegate);

        // When
        unsupportedHttpMethodHandler.handle(httpExchange);

        // Then
        verify(delegate).handle(httpExchange);
    }

    @Test
    void shouldRespondWith404() throws IOException {
        // Given
        when(httpExchange.getRequestMethod()).thenReturn("GET");

        // When
        unsupportedHttpMethodHandler.handle(httpExchange);

        // Then
        verify(httpExchange).sendResponseHeaders(404, -1);
    }

    @ParameterizedTest
    @CsvSource({ "TRACE", "OPTIONS", "HEAD", "POST", "PUT", "CONNECT", "PATCH", "DELETE" })
    void shouldRejectRequestWithHttpMethod(String httpMethod) throws IOException {
        // Given
        when(httpExchange.getRequestMethod()).thenReturn(httpMethod);
        when(httpExchange.getResponseHeaders()).thenReturn(responseHeaders);

        // When
        unsupportedHttpMethodHandler.handle(httpExchange);

        // Then
        verify(httpExchange).sendResponseHeaders(405, -1);
        verify(responseHeaders).add("Allow", "GET");
    }
}