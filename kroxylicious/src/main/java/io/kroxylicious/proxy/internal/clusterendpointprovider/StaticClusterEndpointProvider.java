/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;
import io.kroxylicious.proxy.service.HostPort;

public class StaticClusterEndpointProvider implements ClusterEndpointProvider {

    private final StaticClusterEndpointProviderConfig config;

    public StaticClusterEndpointProvider(StaticClusterEndpointProviderConfig config) {
        this.config = config;
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return config.bootstrapAddress;
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        var addr = config.brokers.get(nodeId);
        if (addr == null) {
            throw new IllegalArgumentException("No broker address known for nodeId %d".formatted(nodeId));
        }
        return addr;
    }

    @Override
    public int getNumberOfBrokerEndpointsToPrebind() {
        return config.brokers.size();
    }

    public static class StaticClusterEndpointProviderConfig extends BaseConfig {
        private final HostPort bootstrapAddress;
        private final Map<Integer, HostPort> brokers;

        public StaticClusterEndpointProviderConfig(HostPort bootstrapAddress, Map<Integer, HostPort> brokers) {
            this.bootstrapAddress = HostPort.parse(bootstrapAddress.toString());

            if (brokers == null) {
                this.brokers = Map.of(0, this.bootstrapAddress);
            }
            else {
                Preconditions.checkArgument(!brokers.isEmpty(), "requires non-empty map of nodeid to broker address mappings");

                var duplicateBrokerAddresses = brokers.values().stream().filter(e -> Collections.frequency(brokers.values(), e) > 1).distinct().toList();
                Preconditions.checkArgument(duplicateBrokerAddresses.isEmpty(), "all broker addresses must be unique (found duplicates: %s)",
                        duplicateBrokerAddresses.stream().map(HostPort::toString).collect(Collectors.joining(",")));
                this.brokers = brokers;
            }
        }
    }

}
