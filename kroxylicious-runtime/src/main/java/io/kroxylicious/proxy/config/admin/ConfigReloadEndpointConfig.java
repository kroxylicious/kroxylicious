/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import java.time.Duration;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the configuration reload HTTP endpoint.
 * When enabled, exposes a POST endpoint at /admin/config/reload that allows
 * triggering dynamic configuration reload without restarting the proxy.
 * <p>
 * WARNING: This endpoint has NO authentication and is INSECURE by design.
 * Use network policies or firewalls to restrict access.
 */
@JsonSerialize
public record ConfigReloadEndpointConfig(
                                         boolean enabled,
                                         @Nullable Duration timeout) {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Create a disabled configuration.
     */
    public static ConfigReloadEndpointConfig createDisabled() {
        return new ConfigReloadEndpointConfig(false, null);
    }

    /**
     * Create an enabled configuration with default timeout.
     */
    public static ConfigReloadEndpointConfig createEnabled() {
        return new ConfigReloadEndpointConfig(true, DEFAULT_TIMEOUT);
    }

    /**
     * Get the timeout value, using default if not specified.
     */
    public Duration getTimeout() {
        return timeout != null ? timeout : DEFAULT_TIMEOUT;
    }
}
