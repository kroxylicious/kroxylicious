/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Static configuration controlling reload behavior. These options are defined
 * in the proxy's YAML configuration and apply to all reload triggers (HTTP, file watcher, operator).
 *
 * @param onFailure What to do when a reload fails. If {@code null}, defaults to {@link OnFailure#ROLLBACK}.
 * @param persistConfigToDisk Whether to persist a successfully reloaded configuration to disk.
 */
public record ReloadOptions(@Nullable OnFailure onFailure, boolean persistConfigToDisk) {

    /**
     * Default reload options: rollback on failure, persist to disk.
     */
    public static final ReloadOptions DEFAULT = new ReloadOptions(OnFailure.ROLLBACK, true);

    /**
     * Returns the effective on-failure behavior, defaulting to {@link OnFailure#ROLLBACK} if not specified.
     *
     * @return the on-failure behavior, never {@code null}
     */
    public OnFailure effectiveOnFailure() {
        return onFailure != null ? onFailure : OnFailure.ROLLBACK;
    }
}
