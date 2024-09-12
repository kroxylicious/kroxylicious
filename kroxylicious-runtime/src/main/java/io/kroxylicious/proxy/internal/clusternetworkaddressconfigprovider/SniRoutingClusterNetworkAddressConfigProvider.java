/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.EXPECTED_TOKEN_SET;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsOnlyExpectedTokens;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsRequiredTokens;

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
    private final Pattern brokerAddressNodeIdCapturingRegex;

    /**
     * Creates the provider.
     *
     * @param config configuration
     */
    public SniRoutingClusterNetworkAddressConfigProvider(SniRoutingClusterNetworkAddressConfigProviderConfig config) {
        this.bootstrapAddress = config.bootstrapAddress;
        this.brokerAddressPattern = config.brokerAddressPattern;
        this.brokerAddressNodeIdCapturingRegex = config.brokerAddressNodeIdCapturingRegex;
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
    public Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
        if (brokerAddress.port() != bootstrapAddress.port()) {
            return null;
        }
        var matcher = brokerAddressNodeIdCapturingRegex.matcher(brokerAddress.host());
        if (matcher.matches()) {
            var nodeId = matcher.group(1);
            try {
                return Integer.valueOf(nodeId);
            }
            catch (NumberFormatException e) {
                throw new IllegalStateException("unexpected exception parsing : '%s'".formatted(nodeId), e);
            }
        }
        return null;
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
    public static class SniRoutingClusterNetworkAddressConfigProviderConfig {

        private final HostPort bootstrapAddress;

        private final String brokerAddressPattern;
        @JsonIgnore
        private final Pattern brokerAddressNodeIdCapturingRegex;

        public SniRoutingClusterNetworkAddressConfigProviderConfig(
                @JsonProperty(required = true)
                HostPort bootstrapAddress,
                @JsonProperty(required = true)
                String brokerAddressPattern
        ) {
            if (bootstrapAddress == null) {
                throw new IllegalArgumentException("bootstrapAddress cannot be null");
            }
            if (brokerAddressPattern == null) {
                throw new IllegalArgumentException("brokerAddressPattern cannot be null");
            }

            validatePortSpecifier(brokerAddressPattern, s -> {
                throw new IllegalArgumentException("brokerAddressPattern cannot have port specifier.  Found port : " + s + " within " + brokerAddressPattern);
            });

            validateStringContainsOnlyExpectedTokens(brokerAddressPattern, EXPECTED_TOKEN_SET, (tok) -> {
                throw new IllegalArgumentException("brokerAddressPattern contains an unexpected replacement token '" + tok + "'");
            });

            validateStringContainsRequiredTokens(brokerAddressPattern, EXPECTED_TOKEN_SET, (tok) -> {
                throw new IllegalArgumentException("brokerAddressPattern must contain at least one nodeId replacement pattern '" + tok + "'");
            });

            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern;
            this.brokerAddressNodeIdCapturingRegex = BrokerAddressPatternUtils.createNodeIdCapturingRegex(brokerAddressPattern);

        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }

    }

}
