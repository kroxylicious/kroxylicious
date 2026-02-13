/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

/**
 * HTTP response payload for configuration reload operations.
 * This record is serialized to JSON and returned to the client.
 */
public record ReloadResponse(
                             boolean success,
                             String message,
                             int clustersModified,
                             int clustersAdded,
                             int clustersRemoved,
                             String timestamp) {

    /**
     * Create a ReloadResponse from a ReloadResult.
     */
    public static ReloadResponse from(ReloadResult result) {
        return new ReloadResponse(
                result.isSuccess(),
                result.getMessage(),
                result.getClustersModified(),
                result.getClustersAdded(),
                result.getClustersRemoved(),
                result.getTimestamp().toString());
    }

    /**
     * Create an error response.
     */
    public static ReloadResponse error(String message) {
        return new ReloadResponse(
                false,
                message,
                0,
                0,
                0,
                java.time.Instant.now().toString());
    }
}
