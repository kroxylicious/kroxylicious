/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the target (upstream) kafka cluster.
 */
public class TargetCluster {

    private final String bootstrapServers;

    public TargetCluster(@JsonProperty(value = "bootstrap_servers") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * A list of host/port pairs to use for establishing the initial connection to the target (upstream) Kafka cluster.
     * This list should be in the form host1:port1,host2:port2,...
     *
     * @return comma separated list of bootstrap servers.
     */
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public String toString() {
        return "TargetCluster [bootstrapServers=" + bootstrapServers + "]";
    }
}
