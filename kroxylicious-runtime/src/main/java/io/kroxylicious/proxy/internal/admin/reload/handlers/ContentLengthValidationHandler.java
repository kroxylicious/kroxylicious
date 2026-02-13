/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload.handlers;

import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestContext;
import io.kroxylicious.proxy.internal.admin.reload.ValidationException;

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;

/**
 * Validates that the request body does not exceed the maximum allowed size.
 * This is a DoS protection measure to prevent excessive memory allocation.
 */
public class ContentLengthValidationHandler implements ReloadRequestHandler {

    private final int maxContentLength;

    /**
     * Create a handler with the specified maximum content length.
     *
     * @param maxContentLength Maximum allowed content length in bytes
     */
    public ContentLengthValidationHandler(int maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

    @Override
    public ReloadRequestContext handle(ReloadRequestContext context) throws ValidationException {
        HttpRequest request = context.getHttpRequest();
        int contentLength = HttpUtil.getContentLength(request, -1);

        if (contentLength < 0) {
            throw new ValidationException("Content-Length header is required");
        }

        if (contentLength == 0) {
            throw new ValidationException("Request body cannot be empty");
        }

        if (contentLength > maxContentLength) {
            throw new ValidationException(
                    "Request body too large: " + contentLength + " bytes. Maximum allowed: " + maxContentLength + " bytes");
        }

        return context;
    }
}
