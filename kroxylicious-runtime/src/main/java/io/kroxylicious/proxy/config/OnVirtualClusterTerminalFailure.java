/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Policy governing proxy behaviour when a virtual cluster reaches the {@code Stopped} terminal state
 * via the {@code failed → stopped} lifecycle edge (i.e. truly unrecoverable failure).
 * <p>
 * This policy fires only on the terminal-failure edge — not on intentional removal
 * ({@code draining → stopped}) or shutdown-during-startup ({@code initializing → stopped}).
 *
 * @param serve how much of the proxy should continue running when a VC is unrecoverable;
 *              defaults to {@link ServePolicy#NONE} if not specified.
 */
public record OnVirtualClusterTerminalFailure(@Nullable ServePolicy serve) {

    private static final ServePolicy DEFAULT_SERVE = ServePolicy.NONE;

    public static final OnVirtualClusterTerminalFailure DEFAULT = new OnVirtualClusterTerminalFailure(null);

    public OnVirtualClusterTerminalFailure {
        if (serve == null) {
            serve = DEFAULT_SERVE;
        }
    }

    /**
     * How much of the proxy continues running when a VC reaches terminal failure.
     */
    public enum ServePolicy {

        /**
         * Any unrecoverable VC shuts down the entire proxy.
         * This is the default and matches current behaviour — configuration errors surface immediately.
         */
        @JsonProperty("none")
        NONE,

        /**
         * Remaining healthy VCs continue serving. The failed VC is reported as stopped and the
         * operator can inspect it and re-apply once the underlying issue is fixed.
         */
        @JsonProperty("successful")
        SUCCESSFUL
    }
}
