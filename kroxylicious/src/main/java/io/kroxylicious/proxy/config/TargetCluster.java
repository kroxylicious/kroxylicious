/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TargetCluster {

    private final String bootstrapServers;

    public TargetCluster(@JsonProperty(value = "bootstrap_servers") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public String toString() {
        return "TargetCluster [bootstrapServers=" + bootstrapServers + "]";
    }
}
