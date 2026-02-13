/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.endpoints;

import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.admin.format.ResponseFormatter;
import io.kroxylicious.proxy.internal.admin.reload.ConcurrentReloadException;
import io.kroxylicious.proxy.internal.admin.reload.ReloadException;
import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestContext;
import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestProcessor;
import io.kroxylicious.proxy.internal.admin.reload.ReloadResponse;
import io.kroxylicious.proxy.internal.admin.reload.ValidationException;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * HTTP POST endpoint for triggering configuration reload at /admin/config/reload.
 * <p>
 * WARNING: This endpoint has NO authentication and is INSECURE by design.
 * Use network policies or firewalls to restrict access.
 * <p>
 * Request format:
 * - Method: POST
 * - Content-Type: application/yaml, text/yaml, or application/x-yaml
 * - Body: YAML configuration
 * <p>
 * Response format:
 * - Content-Type: application/json
 * - Status: 200 OK (success) or 500 Internal Server Error (failure)
 * - Body: JSON with success, message, and change counts
 */
public class ConfigurationReloadEndpoint implements Function<HttpRequest, HttpResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationReloadEndpoint.class);

    /**
     * Endpoint path.
     */
    public static final String PATH = "/admin/config/reload";

    private final ReloadRequestProcessor requestProcessor;
    private final ResponseFormatter responseFormatter;

    /**
     * Create a configuration reload endpoint.
     *
     * @param requestProcessor The request processor
     * @param responseFormatter The response formatter
     */
    public ConfigurationReloadEndpoint(
                                      ReloadRequestProcessor requestProcessor,
                                      ResponseFormatter responseFormatter) {
        this.requestProcessor = Objects.requireNonNull(requestProcessor, "requestProcessor cannot be null");
        this.responseFormatter = Objects.requireNonNull(responseFormatter, "responseFormatter cannot be null");
    }

    @Override
    public HttpResponse apply(HttpRequest request) {
        LOGGER.debug("Configuration reload endpoint invoked");

        try {
            // Create context from request
            ReloadRequestContext context = ReloadRequestContext.from(request);

            // Process request through handler chain
            ReloadResponse response = requestProcessor.process(context);

            // Format and return response
            return responseFormatter.format(response, request);

        }
        catch (ValidationException e) {
            LOGGER.warn("Configuration reload validation failed: {}", e.getMessage());
            return createErrorResponse(request, HttpResponseStatus.BAD_REQUEST,
                    e.getMessage());
        }
        catch (ConcurrentReloadException e) {
            LOGGER.warn("Concurrent reload attempted: {}", e.getMessage());
            return createErrorResponse(request, HttpResponseStatus.CONFLICT,
                    e.getMessage());
        }
        catch (ReloadException e) {
            LOGGER.error("Configuration reload failed", e);
            return createErrorResponse(request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    e.getMessage());
        }
        catch (Exception e) {
            LOGGER.error("Unexpected error during configuration reload", e);
            return createErrorResponse(request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Internal server error: " + e.getMessage());
        }
    }

    /**
     * Create an error response with JSON body.
     */
    private FullHttpResponse createErrorResponse(HttpRequest request, HttpResponseStatus status, String message) {
        ReloadResponse errorResponse = ReloadResponse.error(message);

        try {
            return responseFormatter.format(errorResponse, request);
        }
        catch (Exception e) {
            // Fallback to plain text if JSON formatting fails
            LOGGER.error("Failed to format error response as JSON, falling back to plain text", e);
            return createPlainTextErrorResponse(request, status, message);
        }
    }

    /**
     * Create a plain text error response as final fallback.
     */
    private FullHttpResponse createPlainTextErrorResponse(HttpRequest request, HttpResponseStatus status, String message) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                request.protocolVersion(),
                status,
                Unpooled.wrappedBuffer(message.getBytes(UTF_8)));

        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8")
                .setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        return response;
    }
}
