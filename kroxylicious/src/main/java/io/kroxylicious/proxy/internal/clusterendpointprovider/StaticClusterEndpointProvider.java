/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;

public class StaticClusterEndpointProvider implements ClusterEndpointProvider {

    private final StaticClusterEndpointProviderConfig config;

    public StaticClusterEndpointProvider(StaticClusterEndpointProviderConfig config) {
        this.config = config;
    }

    @Override
    public String getClusterBootstrapAddress() {
        return config.bootstrapAddress;
    }

    @Override
    public String getBrokerAddress(int nodeId) throws IllegalArgumentException {
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
        private final String bootstrapAddress;
        private final Map<Integer, String> brokers;

        public StaticClusterEndpointProviderConfig(String bootstrapAddress, Map<Integer, String> brokers) {
            Preconditions.checkArgument(String.valueOf(bootstrapAddress).split(":").length == 2, "requires bootstrap to have the form 'host:port' (found %s)",
                    bootstrapAddress);
            if (brokers == null) {
                brokers = Map.of(0, bootstrapAddress);
            }
            else {
                Preconditions.checkArgument(!(brokers == null || brokers.isEmpty()), "requires non-empty map of nodeid to broker address mappings");
                brokers.forEach((k, v) -> Preconditions.checkArgument(String.valueOf(v).split(":").length == 2,
                        "require broker address for node %s to have the form 'host:port' (found %s)", k, v));
                int expectedSize = brokers.size();
                var expectedKeys = IntStream.range(0, expectedSize).boxed().collect(Collectors.toSet());
                var actualKeys = new TreeSet<>(brokers.keySet());
                Preconditions.checkArgument(expectedSize == actualKeys.size() && expectedKeys.containsAll(actualKeys),
                        "broker keys must describe a integer range 0..%s (inclusive) (found %s)", expectedSize - 1,
                        actualKeys.stream().map(String::valueOf).collect(Collectors.joining(",")));
                var actualValues = brokers.values();
                var duplicateBrokerAddresses = actualValues.stream().filter(e -> Collections.frequency(actualValues, e) > 1).distinct().toList();
                Preconditions.checkArgument(duplicateBrokerAddresses.isEmpty(), "all broker addresses must be unique (found duplicates: %s)",
                        String.join(",", duplicateBrokerAddresses));
            }
            this.bootstrapAddress = bootstrapAddress;
            this.brokers = brokers;
        }
    }

}
