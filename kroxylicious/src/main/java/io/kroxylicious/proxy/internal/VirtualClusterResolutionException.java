/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

public class VirtualClusterResolutionException extends RuntimeException {
    public VirtualClusterResolutionException(String message) {
        super(message);
    }

    public VirtualClusterResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
