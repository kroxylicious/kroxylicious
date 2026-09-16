/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

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
 */
public record ClusterDefinition(
                                @JsonProperty(required = true) String name,
                                @JsonProperty(required = true) String bootstrapServers,
                                @Nullable Tls tls,
                                @Nullable @JsonProperty("bootstrapServerSelection") BootstrapSelectionStrategy selectionStrategy) {

    /**
     * Validates the cluster definition, stripping whitespace from {@code bootstrapServers}.
     */
    @JsonCreator
    public ClusterDefinition {
        Objects.requireNonNull(name, "'name' is required in a cluster definition");
        Objects.requireNonNull(bootstrapServers, "'bootstrapServers' is required in a cluster definition");
        bootstrapServers = bootstrapServers.replaceAll("\\s", "");
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
     * Converts this definition to a {@link TargetCluster} for use in the runtime.
     * <p>
     * A definition is referenced by many virtual clusters and routes, and this method is called
     * once for each of them, so the returned target cluster gets its own selection strategy via
     * {@link BootstrapSelectionStrategy#newInstance()} rather than sharing this definition's.
     * Sharing would give unrelated virtual clusters common bootstrap selection state.
     *
     * @return a target cluster with the same bootstrap servers and TLS, and its own selection strategy
     */
    public TargetCluster toTargetCluster() {
        return new TargetCluster(bootstrapServers, Optional.ofNullable(tls),
                selectionStrategy == null ? null : selectionStrategy.newInstance());
    }
}
