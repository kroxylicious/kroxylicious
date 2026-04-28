/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonInclude;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Reload-specific configuration — governs how the proxy responds to failures during a
 * {@code applyConfiguration()} call, and whether successful reloads are persisted to disk.
 *
 * @param onFailure     orchestration-level failure policy; defaults to {@code rollback: true} if omitted.
 * @param persistToDisk whether to persist a successful reload back to the configuration file;
 *                      defaults to {@code true} if omitted.
 */
public record ConfigurationReload(@Nullable OnFailure onFailure,
                                  @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Boolean persistToDisk) {

    private static final OnFailure DEFAULT_ON_FAILURE = new OnFailure(null);
    private static final Boolean DEFAULT_PERSIST_TO_DISK = Boolean.TRUE;

    public static final ConfigurationReload DEFAULT = new ConfigurationReload(null, null);

    public ConfigurationReload {
        if (onFailure == null) {
            onFailure = DEFAULT_ON_FAILURE;
        }
        if (persistToDisk == null) {
            persistToDisk = DEFAULT_PERSIST_TO_DISK;
        }
    }

    /**
     * Failure behaviour for reload orchestration. Composed independently of the
     * {@code onVirtualClusterTerminalFailure} policy.
     *
     * @param rollback whether to revert previously-successful operations when any VC fails to
     *                 apply new configuration; defaults to {@code true} if omitted.
     */
    public record OnFailure(@JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Boolean rollback) {

        private static final Boolean DEFAULT_ROLLBACK = Boolean.TRUE;

        public OnFailure {
            if (rollback == null) {
                rollback = DEFAULT_ROLLBACK;
            }
        }
    }
}
