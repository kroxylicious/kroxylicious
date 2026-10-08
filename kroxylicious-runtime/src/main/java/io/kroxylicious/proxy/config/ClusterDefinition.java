/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.bootstrap.BootstrapSelectionStrategy;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A named cluster definition, referenced by routes and virtual clusters.
 *
 * @param name unique name for this cluster
 * @param bootstrapServers comma-separated list of host:port pairs
 * @param tls optional TLS configuration for the upstream connection
 * @param selectionStrategy optional strategy for selecting a bootstrap server when several are listed
 * @param connectTimeout optional maximum time to wait for a TCP connection to an upstream broker to be established;
 *                       when set it must be positive and must not exceed {@code Integer.MAX_VALUE} milliseconds (~24.8 days)
 */
public record ClusterDefinition(
                                @JsonProperty(required = true) String name,
                                @JsonProperty(required = true) String bootstrapServers,
                                @Nullable Tls tls,
                                @Nullable @JsonProperty("bootstrapServerSelection") BootstrapSelectionStrategy selectionStrategy,
                                @Nullable @JsonProperty("connectTimeout") Duration connectTimeout) {

    /**
     * Validates the cluster definition, stripping whitespace from {@code bootstrapServers}.
     */
    @JsonCreator
    public ClusterDefinition {
        Objects.requireNonNull(name, "'name' is required in a cluster definition");
        Objects.requireNonNull(bootstrapServers, "'bootstrapServers' is required in a cluster definition");
        bootstrapServers = bootstrapServers.replaceAll("\\s", "");
        TargetCluster.validateConnectTimeout(connectTimeout, "for cluster definition '" + name + "'");
    }

    /**
     * Convenience constructor with no bootstrap-server selection strategy.
     *
     * @param name unique name for this cluster
     * @param bootstrapServers comma-separated list of host:port pairs
     * @param tls optional TLS configuration for the upstream connection
     */
    public ClusterDefinition(String name, String bootstrapServers, @Nullable Tls tls) {
        this(name, bootstrapServers, tls, null);
    }

    /**
     * Convenience constructor with no connect timeout.
     *
     * @param name unique name for this cluster
     * @param bootstrapServers comma-separated list of host:port pairs
     * @param tls optional TLS configuration for the upstream connection
     * @param selectionStrategy optional strategy for selecting a bootstrap server when several are listed
     */
    public ClusterDefinition(String name, String bootstrapServers, @Nullable Tls tls, @Nullable BootstrapSelectionStrategy selectionStrategy) {
        this(name, bootstrapServers, tls, selectionStrategy, null);
    }

    /**
     * Converts this definition to a {@link TargetCluster} for use in the runtime.
     * <p>
     * A definition is referenced by many virtual clusters and routes, and this method is called
     * once for each of them. The selection strategy is immutable configuration and so is safely
     * shared by every target cluster derived from this definition; the mutable selection state is
     * created separately, per upstream cluster model, via {@link BootstrapSelectionStrategy#newSelector()},
     * so unrelated virtual clusters never share bootstrap selection state.
     *
     * @return a target cluster with the same bootstrap servers, TLS and selection strategy
     */
    public TargetCluster toTargetCluster() {
        return new TargetCluster(bootstrapServers, Optional.ofNullable(tls), selectionStrategy, connectTimeout);
    }
}
