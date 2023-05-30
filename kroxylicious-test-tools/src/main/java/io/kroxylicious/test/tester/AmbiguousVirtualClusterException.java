/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

/**
 * Indicates a command could not be executed because the virtual cluster
 * targeted was ambiguous.
 */
public class AmbiguousVirtualClusterException extends RuntimeException {

    /**
     * Create AmbiguousVirtualClusterException
     * @param message message
     */
    public AmbiguousVirtualClusterException(String message) {
        super(message);
    }
}
