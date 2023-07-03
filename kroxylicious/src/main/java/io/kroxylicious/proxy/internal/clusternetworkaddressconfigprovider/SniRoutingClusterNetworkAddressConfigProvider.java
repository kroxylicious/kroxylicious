/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.LITERAL_NODE_ID;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateTokens;

/**
 * A ClusterNetworkAddressConfigProvider implementation that uses a single, shared, port for bootstrap and
 * all brokers.  SNI information is used to route the connection to the correct target.
 * <br/>
 * The following configuration is required:
 * <ul>
 *    <li>{@code bootstrapAddress} a {@link HostPort} defining the host and port of the bootstrap address.</li>
 *    <li>{@code brokerAddressPattern} an address pattern used to form broker addresses.  It is addresses made from this pattern that are returned to the kafka
 *  *    client in the Metadata response so must be resolvable by the client.  One pattern is supported: {@code $(nodeId)} which interpolates the node id into the address.
 * </ul>
 */
public class SniRoutingClusterNetworkAddressConfigProvider implements ClusterNetworkAddressConfigProvider {

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
        return new HostPort(BrokerAddressPatternUtils.replaceLiteralNodeId(brokerAddressPattern, nodeId), bootstrapAddress.port());
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

            validatePortSpecifier(brokerAddressPattern, s -> {
                throw new IllegalArgumentException("brokerAddressPattern cannot have port specifier.  Found port : " + s + " within " + brokerAddressPattern);
            });

            validateTokens(brokerAddressPattern,
                    Set.of(LITERAL_NODE_ID), (token) -> {
                        throw new IllegalArgumentException("brokerAddressPattern contains an unexpected replacement token '" + token + "'");
                    },
                    Set.of(LITERAL_NODE_ID), (u) -> {
                        throw new IllegalArgumentException("brokerAddressPattern must contain at least one nodeId replacement pattern '" + LITERAL_NODE_ID + "'");
                    });

            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern;
        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }

    }
}
