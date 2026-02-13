/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.format;

import io.kroxylicious.proxy.internal.admin.reload.ReloadResponse;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;

/**
 * Strategy interface for formatting reload responses in different formats.
 * Implementations can provide JSON, YAML, or other response formats.
 */
public interface ResponseFormatter {

    /**
     * Format a reload response as an HTTP response.
     *
     * @param response The reload response to format
     * @param request The original HTTP request (for protocol version, etc.)
     * @return Formatted HTTP response
     */
    FullHttpResponse format(ReloadResponse response, HttpRequest request);

    /**
     * Get the Content-Type header value for this formatter.
     *
     * @return Content-Type value (e.g., "application/json")
     */
    String getContentType();
}
