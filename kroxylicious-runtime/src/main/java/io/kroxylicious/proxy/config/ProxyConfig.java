/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.time.Duration;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Proxy-level configuration block. Controls proxy-wide behaviour that applies
 * across all virtual clusters.
 * <p>
 * This block will be extended in the hot-reload PR with additional settings
 * such as {@code onVirtualClusterTerminalFailure} and {@code configurationReload}.
 *
 * @param drainTimeout maximum time to wait for in-flight requests to complete during
 *                     graceful connection draining. Defaults to 30 seconds if not specified.
 */
public record ProxyConfig(@Nullable Duration drainTimeout) {

    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    public static final ProxyConfig DEFAULT = new ProxyConfig(null);

    public ProxyConfig {
        if (drainTimeout == null) {
            drainTimeout = DEFAULT_DRAIN_TIMEOUT;
        }
        else if (drainTimeout.isZero() || drainTimeout.isNegative()) {
            throw new IllegalConfigurationException("proxy.drainTimeout must be positive, got: " + drainTimeout);
        }
    }
}
