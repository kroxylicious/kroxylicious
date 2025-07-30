/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Management configuration.
 *
 * @param bindAddress bind address for the management interface. If omitted, all network interfaces will be bound.
 * @param port management port
 * @param endpoints management endpoint configuration
 *
 * <br>
 * Note that {@code host} is accepted as an alias for {@code bindAddress}.  Use of {@code host} is deprecated since 0.11.0
 * and will be removed in a future release.
 */
public record ManagementConfiguration(@Nullable @JsonAlias("host") @JsonProperty(value = "bindAddress", required = false) String bindAddress,
                                      @Nullable @JsonProperty(value = "port", required = false) Integer port,
                                      @Nullable @JsonProperty(value = "endpoints", required = false) EndpointsConfiguration endpoints) {

    public static final int DEFAULT_MANAGEMENT_PORT = 9190;
    public static final String DEFAULT_BIND_ADDRESS = "0.0.0.0";

    /**
     * Gets the effective port for management taking into account defaults.
     * @return port
     */
    public int getEffectivePort() {
        return Optional.ofNullable(port).orElse(DEFAULT_MANAGEMENT_PORT);
    }

    /**
     * Gets the effective bind address for management taking into account defaults.
     * @return bind address
     */
    public String getEffectiveBindAddress() {
        return Optional.ofNullable(bindAddress).orElse(DEFAULT_BIND_ADDRESS);
    }
}
