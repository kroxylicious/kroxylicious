/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper.fixedcluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

public class FixedClusterAddressManagerConfig extends BaseConfig {
    @JsonCreator
    public FixedClusterAddressManagerConfig(@JsonProperty(value = "bootstrap_servers") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private final String bootstrapServers;

    public String bootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public String toString() {
        return "FixedClusterAddressMapperFactoryConfig [bootstrapServers=" + bootstrapServers + "]";
    }
}
