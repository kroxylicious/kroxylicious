/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public class StaticClusterEndpointConfigProvider implements ClusterEndpointConfigProvider {

    private static final EndpointMatchResult BOOTSTRAP_MATCHED = new EndpointMatchResult(true, null);
    private static final EndpointMatchResult NO_MATCH = new EndpointMatchResult(false, null);
    private final HostPort bootstrapAddress;
    private final Map<Integer, HostPort> brokers;
    private final Map<Integer, EndpointMatchResult> portToNodeIdMatchedMap;

    public StaticClusterEndpointConfigProvider(StaticClusterEndpointProviderConfig config) {
        this.bootstrapAddress = config.bootstrapAddress;
        this.brokers = config.brokers;
        this.portToNodeIdMatchedMap = this.brokers.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().port(), e -> new EndpointMatchResult(true, e.getKey())));
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
    public EndpointMatchResult hasMatchingEndpoint(String sniHostname, int port) {
        if (bootstrapAddress.port() == port) {
            return BOOTSTRAP_MATCHED;
        }
        return portToNodeIdMatchedMap.getOrDefault(port, NO_MATCH);
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
