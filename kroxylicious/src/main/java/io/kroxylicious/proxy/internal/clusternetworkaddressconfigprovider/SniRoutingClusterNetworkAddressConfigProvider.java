/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A ClusterNetworkAddressConfigProvider implementation that uses a single port for bootstrap and
 * all brokers.  SNI information is used to route the connection to the correct target.
 *
 */
public class SniRoutingClusterNetworkAddressConfigProvider implements ClusterNetworkAddressConfigProvider {

    private static final String LITERAL_NODE_ID = "$(nodeId)";
    private static final Pattern NODE_ID_TOKEN_RE = Pattern.compile(Pattern.quote(LITERAL_NODE_ID));
    private final HostPort bootstrapAddress;
    private final String brokerAddressPattern;

    /**
     * Creates the provider.
     *
     * @param config configuration
     */
    public SniRoutingClusterNetworkAddressConfigProvider(SniRoutingClusterNetworkAddressConfigProviderConfig config) {
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

    /**
     * Creates the configuration for this provider.
     */
    public static class SniRoutingClusterNetworkAddressConfigProviderConfig extends BaseConfig {

        private final HostPort bootstrapAddress;

        private final String brokerAddressPattern;

        public SniRoutingClusterNetworkAddressConfigProviderConfig(@JsonProperty(required = true) HostPort bootstrapAddress,
                                                                   @JsonProperty(required = true) String brokerAddressPattern) {
            if (bootstrapAddress == null) {
                throw new IllegalArgumentException("bootstrapAddress cannot be null");
            }
            if (brokerAddressPattern == null) {
                throw new IllegalArgumentException("brokerAddressPattern cannot be null");
            }

            var matcher = NODE_ID_TOKEN_RE.matcher(brokerAddressPattern);
            if (!matcher.find()) {
                throw new IllegalArgumentException("brokerAddressPattern must contain exactly one nodeId replacement pattern " + LITERAL_NODE_ID + ". Found none.");
            }

            var stripped = matcher.replaceFirst("");
            matcher = NODE_ID_TOKEN_RE.matcher(stripped);
            if (matcher.find()) {
                throw new IllegalArgumentException("brokerAddressPattern must contain exactly one nodeId replacement pattern " + LITERAL_NODE_ID + ". Found too many.");
            }

            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern;
        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }

    }
}
