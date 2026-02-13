/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.format;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.internal.admin.reload.ReloadResponse;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Formats reload responses as JSON.
 * Uses Jackson ObjectMapper for serialization.
 */
public class JsonResponseFormatter implements ResponseFormatter {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonResponseFormatter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public FullHttpResponse format(ReloadResponse response, HttpRequest request) {
        try {
            String json = MAPPER.writeValueAsString(response);
            HttpResponseStatus status = response.success() ?
                    HttpResponseStatus.OK :
                    HttpResponseStatus.INTERNAL_SERVER_ERROR;

            FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                    request.protocolVersion(),
                    status,
                    Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8)));

            httpResponse.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, getContentType())
                    .setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());

            return httpResponse;
        }
        catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize reload response to JSON", e);
            // Return a fallback error response
            return createFallbackErrorResponse(request);
        }
    }

    @Override
    public String getContentType() {
        return "application/json; charset=utf-8";
    }

    /**
     * Create a minimal error response when JSON serialization fails.
     */
    private FullHttpResponse createFallbackErrorResponse(HttpRequest request) {
        String fallbackJson = "{\"success\":false,\"message\":\"Failed to format response\"}";
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                request.protocolVersion(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                Unpooled.wrappedBuffer(fallbackJson.getBytes(StandardCharsets.UTF_8)));

        httpResponse.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, getContentType())
                .setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());

        return httpResponse;
    }
}
