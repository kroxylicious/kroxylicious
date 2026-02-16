/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

import io.kroxylicious.proxy.config.Configuration;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

/**
 * Immutable context object passed through the request processing chain.
 * Each handler can create an updated context with additional information
 * without modifying the original context.
 * <p>
 * Uses the immutable builder pattern where each "with" method returns
 * a new instance with the updated field.
 */
public class ReloadRequestContext {

    private final HttpRequest httpRequest;
    private final String requestBody;
    private final Configuration parsedConfiguration;
    private final ReloadResponse response;

    private ReloadRequestContext(Builder builder) {
        this.httpRequest = Objects.requireNonNull(builder.httpRequest, "httpRequest cannot be null");
        this.requestBody = builder.requestBody;
        this.parsedConfiguration = builder.parsedConfiguration;
        this.response = builder.response;
    }

    /**
     * Create initial context from HTTP request.
     */
    public static ReloadRequestContext from(HttpRequest request) {
        String body = null;
        if (request instanceof FullHttpRequest fullRequest) {
            ByteBuf content = fullRequest.content();
            if (content.readableBytes() > 0) {
                body = content.toString(StandardCharsets.UTF_8);
            }
        }

        return new Builder()
                .withHttpRequest(request)
                .withRequestBody(body)
                .build();
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    @Nullable
    public String getRequestBody() {
        return requestBody;
    }

    @Nullable
    public Configuration getParsedConfiguration() {
        return parsedConfiguration;
    }

    @Nullable
    public ReloadResponse getResponse() {
        return response;
    }

    /**
     * Create a new context with parsed configuration.
     */
    public ReloadRequestContext withParsedConfiguration(Configuration config) {
        return new Builder(this).withParsedConfiguration(config).build();
    }

    /**
     * Create a new context with reload response.
     */
    public ReloadRequestContext withResponse(ReloadResponse response) {
        return new Builder(this).withResponse(response).build();
    }

    public static class Builder {
        private HttpRequest httpRequest;
        private String requestBody;
        private Configuration parsedConfiguration;
        private ReloadResponse response;

        public Builder() {
        }

        /**
         * Copy constructor for creating modified contexts.
         */
        public Builder(ReloadRequestContext context) {
            this.httpRequest = context.httpRequest;
            this.requestBody = context.requestBody;
            this.parsedConfiguration = context.parsedConfiguration;
            this.response = context.response;
        }

        public Builder withHttpRequest(HttpRequest httpRequest) {
            this.httpRequest = httpRequest;
            return this;
        }

        public Builder withRequestBody(String requestBody) {
            this.requestBody = requestBody;
            return this;
        }

        public Builder withParsedConfiguration(Configuration parsedConfiguration) {
            this.parsedConfiguration = parsedConfiguration;
            return this;
        }

        public Builder withResponse(ReloadResponse response) {
            this.response = response;
            return this;
        }

        public ReloadRequestContext build() {
            return new ReloadRequestContext(this);
        }
    }
}
