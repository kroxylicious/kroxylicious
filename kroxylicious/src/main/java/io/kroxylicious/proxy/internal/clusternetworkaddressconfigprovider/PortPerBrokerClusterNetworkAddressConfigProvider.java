/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.EXPECTED_TOKEN_SET;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsOnlyExpectedTokens;

/**
 * A ClusterNetworkAddressConfigProvider implementation that uses a separate port per broker endpoint.
 * <br/>
 * The following configuration is supported:
 * <ul>
 *    <li>{@code bootstrapAddress} (required) a {@link HostPort} defining the host and port of the bootstrap address.</li>
 *    <li>{@code brokerAddressPattern} (optional) an address pattern used to form broker addresses.  It is addresses made from this pattern that are returned to the kafka
 *    client in the Metadata response so must be resolvable by the client.  One pattern is supported: {@code $(nodeId)} which interpolates the node id into the address.
 *    If brokerAddressPattern is omitted, it defaulted it based on the host name of {@code bootstrapAddress}.</li>
 *    <li>{@code brokerStartPort} (optional) defines the starting range of port number that will be assigned to the brokers.  If omitted, it is defaulted to
 *    the port number of {@code bootstrapAddress + 1}.</li>
 *    <li>{@code numberOfBrokerPorts} (optional) defines the maximum number of broker ports that will be permitted. If omitted, it is defaulted to {$code 3}.</li>
 * </ul>
 */
public class PortPerBrokerClusterNetworkAddressConfigProvider implements ClusterNetworkAddressConfigProvider {

    private final HostPort bootstrapAddress;
    private final String brokerAddressPattern;
    private final int brokerStartPort;
    private final Set<Integer> exclusivePorts;
    private final int brokerEndPortExclusive;
    private final int numberOfBrokerPorts;

    /**
     * Creates the provider.
     *
     * @param config configuration
     */
    public PortPerBrokerClusterNetworkAddressConfigProvider(PortPerBrokerClusterNetworkAddressConfigProviderConfig config) {
        this.bootstrapAddress = config.bootstrapAddress;
        this.brokerAddressPattern = config.brokerAddressPattern;
        this.brokerStartPort = config.brokerStartPort;
        this.numberOfBrokerPorts = config.numberOfBrokerPorts;
        this.brokerEndPortExclusive = brokerStartPort + numberOfBrokerPorts;

        var exclusivePorts = IntStream.range(brokerStartPort, brokerEndPortExclusive).boxed().collect(Collectors.toCollection(HashSet::new));
        exclusivePorts.add(bootstrapAddress.port());
        this.exclusivePorts = Collections.unmodifiableSet(exclusivePorts);
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return this.bootstrapAddress;
    }

    @Override

    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        int port = brokerStartPort + nodeId;
        if (port >= brokerEndPortExclusive) {
            throw new IllegalArgumentException(
                    "Cannot generate broker address for node id %d as port %d would fall outside port range %d-%d that is defined for provider with downstream bootstrap %s)"
                            .formatted(
                                    nodeId,
                                    port,
                                    brokerStartPort,
                                    brokerEndPortExclusive - 1,
                                    bootstrapAddress));
        }

        return new HostPort(BrokerAddressPatternUtils.replaceLiteralNodeId(brokerAddressPattern, nodeId), port);
    }

    @Override
    public Set<Integer> getExclusivePorts() {
        return this.exclusivePorts;
    }

    @Override
    public Map<Integer, HostPort> discoveryAddressMap() {
        return IntStream.range(0, numberOfBrokerPorts).boxed().collect(Collectors.toMap(Function.identity(), this::getBrokerAddress));
    }

    /**
     * Creates the configuration for this provider.
     */
    public static class PortPerBrokerClusterNetworkAddressConfigProviderConfig {
        private final HostPort bootstrapAddress;
        private final String brokerAddressPattern;
        private final int brokerStartPort;
        private final int numberOfBrokerPorts;

        public PortPerBrokerClusterNetworkAddressConfigProviderConfig(@JsonProperty(required = true) HostPort bootstrapAddress,
                                                                      @JsonProperty(required = false) String brokerAddressPattern,
                                                                      @JsonProperty(required = false) Integer brokerStartPort,
                                                                      @JsonProperty(required = false, defaultValue = "3") Integer numberOfBrokerPorts) {
            Objects.requireNonNull(bootstrapAddress, "bootstrapAddress cannot be null");

            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern != null ? brokerAddressPattern : bootstrapAddress.host();
            this.brokerStartPort = brokerStartPort != null ? brokerStartPort : (bootstrapAddress.port() + 1);
            this.numberOfBrokerPorts = numberOfBrokerPorts != null ? numberOfBrokerPorts : 3;

            if (this.brokerAddressPattern.isBlank()) {
                throw new IllegalArgumentException("brokerAddressPattern cannot be blank");
            }

            validatePortSpecifier(this.brokerAddressPattern, s -> {
                throw new IllegalArgumentException("brokerAddressPattern cannot have port specifier.  Found port : " + s + " within " + this.brokerAddressPattern);
            });

            if (this.brokerStartPort < 1) {
                throw new IllegalArgumentException("brokerStartPort cannot be less than 1");
            }
            if (this.numberOfBrokerPorts < 1) {
                throw new IllegalArgumentException("numberOfBrokerPorts cannot be less than 1");
            }

            IntStream.range(this.brokerStartPort, this.brokerStartPort + this.numberOfBrokerPorts).filter(i -> i == bootstrapAddress.port()).findFirst().ifPresent(i -> {
                throw new IllegalArgumentException("the port used by the bootstrap address (%d) collides with the broker port range".formatted(bootstrapAddress.port()));
            });

            validateStringContainsOnlyExpectedTokens(this.brokerAddressPattern, EXPECTED_TOKEN_SET, (token) -> {
                throw new IllegalArgumentException("brokerAddressPattern contains an unexpected replacement token '" + token + "'");
            });
        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }
    }

}
