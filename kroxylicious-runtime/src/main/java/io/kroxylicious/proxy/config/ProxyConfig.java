/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Proxy-level configuration block. Controls proxy-wide behaviour that applies across all
 * virtual clusters.
 * <p>
 * The block currently houses two independent policy dimensions introduced by the hot-reload
 * design:
 * <ul>
 *   <li>{@link #onVirtualClusterTerminalFailure()} — lifecycle policy on the
 *       {@code failed → stopped} edge (fires at startup and during reload).</li>
 *   <li>{@link #configurationReload()} — reload-specific policy: what to do when a reload
 *       fails, and whether to persist successful reloads back to disk.</li>
 * </ul>
 *
 * @param onVirtualClusterTerminalFailure terminal-failure policy; defaults to
 *                                        {@link OnVirtualClusterTerminalFailure#DEFAULT}
 *                                        ({@code serve: none}) if omitted.
 * @param configurationReload             reload orchestration policy; defaults to
 *                                        {@link ConfigurationReload#DEFAULT}
 *                                        ({@code rollback: true, persistToDisk: true}) if omitted.
 */
public record ProxyConfig(@Nullable OnVirtualClusterTerminalFailure onVirtualClusterTerminalFailure,
                          @Nullable ConfigurationReload configurationReload) {

    public static final ProxyConfig DEFAULT = new ProxyConfig(null, null);

    public ProxyConfig {
        if (onVirtualClusterTerminalFailure == null) {
            onVirtualClusterTerminalFailure = OnVirtualClusterTerminalFailure.DEFAULT;
        }
        if (configurationReload == null) {
            configurationReload = ConfigurationReload.DEFAULT;
        }
    }
}
