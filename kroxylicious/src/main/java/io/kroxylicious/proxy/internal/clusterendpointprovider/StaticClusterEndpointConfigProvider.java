/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public class StaticClusterEndpointConfigProvider implements ClusterEndpointConfigProvider {

    private final HostPort bootstrapAddress;
    private final Map<Integer, HostPort> brokers;
    private final Set<Integer> ports;

    public StaticClusterEndpointConfigProvider(StaticClusterEndpointProviderConfig config) {
        this.bootstrapAddress = config.bootstrapAddress;
        this.brokers = config.brokers;
        this.ports = brokers.values().stream().map(HostPort::port).collect(Collectors.toCollection(HashSet::new));
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return this.bootstrapAddress;
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        var addr = this.brokers.get(nodeId);
        if (addr == null) {
            throw new IllegalArgumentException("No broker address known for nodeId %d".formatted(nodeId));
        }
        return addr;
    }

    @Override
    public int getNumberOfBrokerEndpointsToPrebind() {
        return this.brokers.size();
    }

    @Override
    public Set<Integer> getExclusivePorts() {
        ports.add(bootstrapAddress.port());
        return ports;
    }

    public static class StaticClusterEndpointProviderConfig extends BaseConfig {
        private final HostPort bootstrapAddress;
        private final Map<Integer, HostPort> brokers;

        public StaticClusterEndpointProviderConfig(HostPort bootstrapAddress, Map<Integer, HostPort> brokers) {
            if (bootstrapAddress == null) {
                throw new IllegalArgumentException("bootstrapAddress cannot be null");
            }
            this.bootstrapAddress = bootstrapAddress;
            if (brokers == null) {
                this.brokers = Map.of(0, this.bootstrapAddress);
            }
            else {
                if (brokers.isEmpty()) {
                    throw new IllegalArgumentException("requires non-empty map of nodeid to broker address mappings");
                }

                var duplicateBrokerAddresses = brokers.values().stream().filter(e -> Collections.frequency(brokers.values(), e) > 1).distinct().toList();
                if (!duplicateBrokerAddresses.isEmpty()) {
                    throw new IllegalArgumentException("all broker addresses must be unique (found duplicates: %s)".formatted(
                            duplicateBrokerAddresses.stream().map(HostPort::toString).collect(Collectors.joining(","))));
                }
                var allPorts = brokers.values().stream().map(HostPort::port).toList();
                var duplicatePorts = allPorts.stream().filter(e -> Collections.frequency(allPorts, e) > 1).distinct().toList();
                if (!duplicatePorts.isEmpty()) {
                    throw new IllegalArgumentException("all broker addresses must have unique ports (found duplicates: %s)".formatted(
                            brokers.values().stream().filter(hp -> duplicatePorts.contains(hp.port())).map(HostPort::toString).collect(Collectors.joining(","))));
                }
                this.brokers = brokers;
            }
        }
    }

}
