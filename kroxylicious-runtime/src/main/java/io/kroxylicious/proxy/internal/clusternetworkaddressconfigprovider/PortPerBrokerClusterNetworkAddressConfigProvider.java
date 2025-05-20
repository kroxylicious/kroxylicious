/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProviderService;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

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
 *
 * @deprecated use {@link io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy} instead
 */
@Plugin(configType = PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig.class)
@Deprecated(since = "0.11.0", forRemoval = true)
public class PortPerBrokerClusterNetworkAddressConfigProvider
        implements ClusterNetworkAddressConfigProviderService<PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig> {

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider build(PortPerBrokerClusterNetworkAddressConfigProviderConfig config, VirtualClusterModel virtualCluster) {
        return new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider().build(config.rangeAwareConfig, virtualCluster);
    }

    /**
     * Creates the configuration for this provider.
     */
    public static class PortPerBrokerClusterNetworkAddressConfigProviderConfig {
        @JsonIgnore
        private final RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig rangeAwareConfig;
        private final HostPort bootstrapAddress;

        @SuppressWarnings("java:S1068") // included for serialization fidelity
        private final String brokerAddressPattern;
        private final int brokerStartPort;
        private final int lowestTargetBrokerId;
        private final int numberOfBrokerPorts;

        public PortPerBrokerClusterNetworkAddressConfigProviderConfig(@JsonProperty(required = true) HostPort bootstrapAddress,
                                                                      @JsonProperty(required = false) String brokerAddressPattern,
                                                                      @JsonProperty(required = false) Integer brokerStartPort,
                                                                      @JsonProperty(required = false, defaultValue = "0") Integer lowestTargetBrokerId,
                                                                      @JsonProperty(required = false, defaultValue = "3") Integer numberOfBrokerPorts) {
            Objects.requireNonNull(bootstrapAddress, "bootstrapAddress cannot be null");
            this.bootstrapAddress = bootstrapAddress;
            this.brokerAddressPattern = brokerAddressPattern != null ? brokerAddressPattern : bootstrapAddress.host();
            this.brokerStartPort = brokerStartPort != null ? brokerStartPort : (bootstrapAddress.port() + 1);
            this.lowestTargetBrokerId = lowestTargetBrokerId != null ? lowestTargetBrokerId : 0;
            this.numberOfBrokerPorts = numberOfBrokerPorts != null ? numberOfBrokerPorts : 3;

            if (this.brokerStartPort < 1) {
                throw new IllegalArgumentException("brokerStartPort cannot be less than 1");
            }
            if (this.numberOfBrokerPorts < 1) {
                throw new IllegalArgumentException("numberOfBrokerPorts cannot be less than 1");
            }
            rangeAwareConfig = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                    this.bootstrapAddress,
                    this.brokerAddressPattern,
                    this.brokerStartPort,
                    List.of(new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.NamedRangeSpec("brokers",
                            new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.IntRangeSpec(this.lowestTargetBrokerId,
                                    this.lowestTargetBrokerId + this.numberOfBrokerPorts))));
        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }

        public String getBrokerAddressPattern() {
            return brokerAddressPattern;
        }

        public int getBrokerStartPort() {
            return brokerStartPort;
        }

    }

}
