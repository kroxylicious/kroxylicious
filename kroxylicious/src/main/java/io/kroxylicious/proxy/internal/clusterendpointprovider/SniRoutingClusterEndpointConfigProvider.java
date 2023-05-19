/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Set;
import java.util.regex.Pattern;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public class SniRoutingClusterEndpointConfigProvider implements ClusterEndpointConfigProvider {

    private static final Pattern NODE_ID_TOKEN = Pattern.compile("\\$\\(nodeId\\)");
    public static final String LITERAL_NODE_ID = "$(nodeId)";
    private final HostPort bootstrapAddress;
    private final String brokerAddressPattern;

    public SniRoutingClusterEndpointConfigProvider(SniRoutingClusterEndpointProviderConfig config) {
        this.bootstrapAddress = config.bootstrapAddress;
        this.brokerAddressPattern = config.brokerAddressPattern;
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return this.bootstrapAddress;
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) {
        if (nodeId < 0) {
            // nodeIds of < 0 have special meaning to kafka.
            throw new IllegalArgumentException("nodeId cannot be less than zero");
        }
        // TODO: consider introducing an cache (LRU?)
        return new HostPort(brokerAddressPattern.replace(LITERAL_NODE_ID, Integer.toString(nodeId)), bootstrapAddress.port());
    }

    @Override
    public Set<Integer> getSharedPorts() {
        return Set.of(bootstrapAddress.port());
    }

    @Override
    public boolean requiresTls() {
        return true;
    }

    public static class SniRoutingClusterEndpointProviderConfig extends BaseConfig {
        private final Pattern brokerAddressPatternRegExp;

        private final HostPort bootstrapAddress;
        private final String brokerAddressPattern;

        public SniRoutingClusterEndpointProviderConfig(HostPort bootstrapAddress, String brokerAddressPattern) {
            if (bootstrapAddress == null) {
                throw new IllegalArgumentException("bootstrapAddress cannot be null");
            }
            if (brokerAddressPattern == null) {
                throw new IllegalArgumentException("brokerAddressPattern cannot be null");
            }

            var matcher = NODE_ID_TOKEN.matcher(brokerAddressPattern);
            if (!matcher.find()) {
                throw new IllegalArgumentException("brokerAddressPattern must contain exactly one nodeId replacement pattern " + LITERAL_NODE_ID + ". Found none.");
            }

            var stripped = matcher.replaceFirst("");
            matcher = NODE_ID_TOKEN.matcher(stripped);
            if (matcher.find()) {
                throw new IllegalArgumentException("brokerAddressPattern must contain exactly one nodeId replacement pattern " + LITERAL_NODE_ID + ". Found too many.");
            }

            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern;
            this.brokerAddressPatternRegExp = Pattern.compile("\\Q" + NODE_ID_TOKEN.matcher(brokerAddressPattern).replaceFirst("\\\\E(\\\\d+)\\\\Q") + "\\E",
                    Pattern.CASE_INSENSITIVE);
        }
    }
}
