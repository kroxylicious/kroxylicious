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

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A named cluster definition, referenced by routes and virtual clusters.
 *
 * @param name unique name for this cluster
 * @param bootstrapServers comma-separated list of host:port pairs
 * @param tls optional TLS configuration for the upstream connection
 */
public record ClusterDefinition(
                                @JsonProperty(required = true) String name,
                                @JsonProperty(required = true) String bootstrapServers,
                                @Nullable Tls tls) {

    @JsonCreator
    public ClusterDefinition {
        Objects.requireNonNull(name, "'name' is required in a cluster definition");
        Objects.requireNonNull(bootstrapServers, "'bootstrapServers' is required in a cluster definition");
        bootstrapServers = bootstrapServers.replaceAll("\\s", "");
    }

    /**
     * Converts this definition to a {@link TargetCluster} for use in the runtime.
     */
    public TargetCluster toTargetCluster() {
        return new TargetCluster(bootstrapServers, Optional.ofNullable(tls));
    }
}
