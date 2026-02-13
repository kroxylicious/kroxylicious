/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload.handlers;

import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestContext;
import io.kroxylicious.proxy.internal.admin.reload.ValidationException;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;

/**
 * Validates that the request has the correct Content-Type header.
 * Expects "application/yaml" or "text/yaml" for YAML configuration.
 */
public class ContentTypeValidationHandler implements ReloadRequestHandler {

    private static final String YAML_CONTENT_TYPE_1 = "application/yaml";
    private static final String YAML_CONTENT_TYPE_2 = "text/yaml";
    private static final String YAML_CONTENT_TYPE_3 = "application/x-yaml";

    @Override
    public ReloadRequestContext handle(ReloadRequestContext context) throws ValidationException {
        HttpRequest request = context.getHttpRequest();
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);

        if (contentType == null || contentType.isEmpty()) {
            throw new ValidationException("Content-Type header is required");
        }

        // Handle content type with charset (e.g., "application/yaml; charset=utf-8")
        String baseContentType = contentType.split(";")[0].trim().toLowerCase();

        if (!YAML_CONTENT_TYPE_1.equals(baseContentType) &&
                !YAML_CONTENT_TYPE_2.equals(baseContentType) &&
                !YAML_CONTENT_TYPE_3.equals(baseContentType)) {
            throw new ValidationException(
                    "Invalid Content-Type: " + contentType + ". Expected application/yaml, text/yaml, or application/x-yaml");
        }

        return context;
    }
}
