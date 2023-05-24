/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A ClusterEndpointConfigProvider implementation that uses a separate port per broker endpoint.
 * <br/>
 * The following configuration is supported:
 * <ul>
 *    <li>{@code bootstrapAddress} (required) a {@link HostPort} defining the host and port of the bootstrap address.</li>
 *    <li>{@code brokerAddressPattern} (optional) an address pattern used to form broker addresses.  It is these address that are returned to the kafka client
 *    in the Metadata response so must be resolvable by the client.  Two patterns are supported: {@code $(portNumber)} (mandatory) and {@code $(nodeId)}
 *    which provide the port number and node id respectively.  If brokerAddressPattern is omitted, it defaulted it based on the host name of {@code bootstrapAddress}.</li>
 *    <li>{@code brokerStartPort} (optional) defines the starting range of port number that will be assigned to the brokers.  If omitted, it is defaulted to
 *    the port number of {@code bootstrapAddress + 1}.</li>
 *    <li>{@code numberOfBrokerPorts} (optional) defines the maximum number of broker ports that will be permitted. If omitted, it is defaulted to {$code 3}.</li>
 * </ul>
 */
public class PortPerBrokerClusterEndpointConfigProvider implements ClusterEndpointConfigProvider {

    private final HostPort bootstrapAddress;

    private static final String LITERAL_PORT_NUMBER = "$(portNumber)";
    private static final String LITERAL_NODE_ID = "$(nodeId)";
    private static final Pattern ANCHORED_PORT_NUMBER_TOKEN_RE = Pattern.compile(":" + Pattern.quote(LITERAL_PORT_NUMBER) + "$");
    private final String brokerAddressPattern;
    private final int brokerStartPort;
    private final int numberOfBrokerPorts;
    private final Set<Integer> exclusivePorts;
    private final int brokerEndPortExclusive;

    /**
     * Creates the provider.
     *
     * @param config configuration
     */
    public PortPerBrokerClusterEndpointConfigProvider(PortPerBrokerClusterEndpointProviderConfig config) {
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
        return HostPort.parse(brokerAddressPattern.replace(LITERAL_PORT_NUMBER, Integer.toString(port)).replace(LITERAL_NODE_ID, Integer.toString(nodeId)));
    }

    @Override
    public Set<Integer> getExclusivePorts() {
        return this.exclusivePorts;
    }

    /**
     * Creates the configuration for this provider.
     */
    public static class PortPerBrokerClusterEndpointProviderConfig extends BaseConfig {
        private final HostPort bootstrapAddress;
        private final String brokerAddressPattern;
        private final int brokerStartPort;
        private final int numberOfBrokerPorts;

        public PortPerBrokerClusterEndpointProviderConfig(@JsonProperty(required = true) HostPort bootstrapAddress,
                                                          @JsonProperty(required = false) String brokerAddressPattern,
                                                          @JsonProperty(required = false) Integer brokerStartPort,
                                                          @JsonProperty(required = false, defaultValue = "3") Integer numberOfBrokerPorts) {
            Objects.requireNonNull(bootstrapAddress, "bootstrapAddress cannot be null");

            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern != null ? brokerAddressPattern : (bootstrapAddress.host() + ":" + LITERAL_PORT_NUMBER);
            this.brokerStartPort = brokerStartPort != null ? brokerStartPort : (bootstrapAddress.port() + 1);
            this.numberOfBrokerPorts = numberOfBrokerPorts != null ? numberOfBrokerPorts : 3;

            if (this.brokerStartPort < 1) {
                throw new IllegalArgumentException("brokerStartPort cannot be less than 1");
            }
            if (this.numberOfBrokerPorts < 1) {
                throw new IllegalArgumentException("brokerStartPort cannot be less than 1");
            }

            IntStream.range(this.brokerStartPort, this.brokerStartPort + this.numberOfBrokerPorts).filter(i -> i == bootstrapAddress.port()).findFirst().ifPresent(i -> {
                throw new IllegalArgumentException("the port used by the bootstrap address (%d) collides with the broker port range".formatted(bootstrapAddress.port()));
            });

            var matcher = ANCHORED_PORT_NUMBER_TOKEN_RE.matcher(this.brokerAddressPattern);
            if (!matcher.find()) {
                throw new IllegalArgumentException("brokerAddressPattern must contain exactly one replacement pattern " + LITERAL_PORT_NUMBER + ". Found none.");
            }
            var stripped = matcher.replaceFirst("");
            matcher = ANCHORED_PORT_NUMBER_TOKEN_RE.matcher(stripped);
            if (matcher.find()) {
                throw new IllegalArgumentException(
                        "brokerAddressPattern must contain exactly one replacement pattern " + LITERAL_PORT_NUMBER + ". Found too many.");
            }

        }
    }

}
