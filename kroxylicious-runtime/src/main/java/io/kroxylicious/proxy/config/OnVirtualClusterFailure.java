/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for how the proxy handles virtual cluster failures during startup.
 *
 * @param serve the failure policy to apply
 */
public record OnVirtualClusterFailure(@Nullable VirtualClusterFailurePolicy serve) {

    public OnVirtualClusterFailure {
        if (serve == null) {
            serve = VirtualClusterFailurePolicy.NONE;
        }
    }
}
