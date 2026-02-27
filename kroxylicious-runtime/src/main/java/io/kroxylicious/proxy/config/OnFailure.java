/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

/**
 * Defines the behavior when a configuration reload fails.
 * <ul>
 *   <li>{@link #ROLLBACK} - Roll back to the previous configuration (default). The proxy continues running with the old config.</li>
 *   <li>{@link #TERMINATE} - Commit partial changes and initiate proxy shutdown. Use when a failed reload indicates an unrecoverable state.</li>
 *   <li>{@link #CONTINUE} - Commit partial changes and continue running. The proxy may be in a partially-updated state.</li>
 * </ul>
 */
public enum OnFailure {
    ROLLBACK,
    TERMINATE,
    CONTINUE
}
