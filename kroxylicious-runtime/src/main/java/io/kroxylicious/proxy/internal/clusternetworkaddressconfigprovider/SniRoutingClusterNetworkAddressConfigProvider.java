/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.PatternAndPort;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProviderService;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.LITERAL_NODE_ID;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.LITERAL_VIRTUAL_CLUSTER_NAME;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsOnlyExpectedTokens;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsRequiredTokens;

/**
 * A ClusterNetworkAddressConfigProvider implementation that binds to a single, shared, port for bootstrap and
 * all brokers. SNI information is used to route the connection to the correct target.
 * <br/>
 * The following configuration is required:
 * <ul>
 *    <li>{@code bootstrapAddress} a {@link HostPort} defining the host and port of the bootstrap address.</li>
 *    <li>${@code advertisedBrokerAddressPattern} must be set. This property contains an address
 *    pattern used to advertise broker addresses.  It is addresses made from this pattern that are returned to the kafka client in the Metadata response so must be
 *    resolvable by the client.  Optionally these properties can specify a port which will be advertised to the clients.  Two patterns ar supported:
 *    <ul>
 *        <li>{@code $(nodeId)} - which interpolates the node id into the address.</li>
 *        <li>{@code $(virtualClusterName)} - which interpolates the virtual cluster name into the address.</li>
 *    </ul>
 * </ul>
 *
 * @deprecated use {@link io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy} instead
 */
@Plugin(configType = SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig.class)
@Deprecated(since = "0.11.0", forRemoval = true)
public class SniRoutingClusterNetworkAddressConfigProvider implements
        ClusterNetworkAddressConfigProviderService<SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig> {
    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider build(SniRoutingClusterNetworkAddressConfigProviderConfig config, VirtualClusterModel virtualCluster) {
        return new Provider(config, virtualCluster);
    }

    private static class Provider implements ClusterNetworkAddressConfigProvider {

        private final HostPort bootstrapAddress;
        private final String brokerAddressPattern;
        private final Pattern brokerAddressNodeIdCapturingRegex;
        private final int advertisedPort;

        /**
         * Creates the provider.
         *
         * @param config configuration
         * @param virtualCluster
         */
        private Provider(SniRoutingClusterNetworkAddressConfigProviderConfig config, VirtualClusterModel virtualCluster) {
            this.bootstrapAddress = new HostPort(
                    BrokerAddressPatternUtils.replaceVirtualClusterName(config.parsedBootstrapAddressPattern, virtualCluster.getClusterName()),
                    config.getBootstrapPort());
            try {
                URI.create(bootstrapAddress.toString());
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Bootstrap address is not a valid URI: " + bootstrapAddress, e);
            }
            this.brokerAddressPattern = BrokerAddressPatternUtils.replaceVirtualClusterName(config.parsedBrokerAddressPattern, virtualCluster.getClusterName());
            this.brokerAddressNodeIdCapturingRegex = buildNodeIdCaptureRegex(config, virtualCluster);
            this.advertisedPort = config.getAdvertisedPort();
            HostPort advertisedBrokerAddress = getAdvertisedBrokerAddress(1);
            try {
                URI.create(advertisedBrokerAddress.toString());
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Advertised broker address pattern did not produce a valid URI for test node 1: " + advertisedBrokerAddress, e);
            }
            validateAddresses();
        }

        private void validateAddresses() {
            HostPort clusterBootstrapAddress = getClusterBootstrapAddress();
            URI.create(clusterBootstrapAddress.toString());
            HostPort advertisedBrokerAddress = getAdvertisedBrokerAddress(1);
            URI.create(advertisedBrokerAddress.toString());
        }

        private Pattern buildNodeIdCaptureRegex(SniRoutingClusterNetworkAddressConfigProviderConfig config, VirtualClusterModel virtualCluster) {
            return Pattern.compile(BrokerAddressPatternUtils.createNodeIdCapturingRegexPattern(config.parsedBrokerAddressPattern, virtualCluster.getClusterName()),
                    Pattern.CASE_INSENSITIVE);
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
        public HostPort getAdvertisedBrokerAddress(int nodeId) {
            HostPort brokerAddress = getBrokerAddress(nodeId);
            return new HostPort(brokerAddress.host(), advertisedPort);
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
        public boolean requiresServerNameIndication() {
            return true;
        }

    }

    /**
     * Creates the configuration for this provider.
     */
    public static class SniRoutingClusterNetworkAddressConfigProviderConfig {

        private static final Set<String> REQUIRED_TOKEN_SET = Set.of(LITERAL_NODE_ID);
        private static final Set<String> ALLOWED_TOKEN_SET = Set.of(LITERAL_NODE_ID, LITERAL_VIRTUAL_CLUSTER_NAME);

        @JsonIgnore
        private final String parsedBootstrapAddressPattern;
        @JsonIgnore
        private final String parsedBrokerAddressPattern;
        @JsonIgnore
        private final Integer advertisedPort;
        @JsonIgnore
        private final Integer bootstrapPort;
        // present for serialize/deserialize fidelity
        @SuppressWarnings("unused")
        private final String advertisedBrokerAddressPattern;

        public SniRoutingClusterNetworkAddressConfigProviderConfig(@JsonProperty(required = true) String bootstrapAddress,
                                                                   @JsonProperty(required = true) String advertisedBrokerAddressPattern) {
            this.advertisedBrokerAddressPattern = advertisedBrokerAddressPattern;
            if (bootstrapAddress == null) {
                throw new IllegalArgumentException("bootstrapAddress cannot be null");
            }
            if (advertisedBrokerAddressPattern == null) {
                throw new IllegalArgumentException("advertisedBrokerAddressPattern cannot be null");
            }

            PatternAndPort bootstrapPatternAndPort = BrokerAddressPatternUtils.parse(bootstrapAddress);
            Optional<Integer> maybeBootstrapPort = bootstrapPatternAndPort.port();
            if (maybeBootstrapPort.isEmpty()) {
                throw new IllegalArgumentException("bootstrapAddress " + bootstrapAddress + " does not contain a port");
            }
            bootstrapPort = maybeBootstrapPort.get();
            PatternAndPort patternAndPort = BrokerAddressPatternUtils.parse(advertisedBrokerAddressPattern);
            String brokerAddressPatternPart = patternAndPort.addressPattern();
            advertisedPort = patternAndPort.port().orElse(bootstrapPort);

            validatePortSpecifier(brokerAddressPatternPart, s -> {
                throw new IllegalArgumentException(
                        "advertisedBrokerAddressPattern address pattern cannot have port specifier.  Found port : " + s + " within " + brokerAddressPatternPart);
            });

            validateStringContainsOnlyExpectedTokens(brokerAddressPatternPart, ALLOWED_TOKEN_SET, tok -> {
                throw new IllegalArgumentException("advertisedBrokerAddressPattern contains an unexpected replacement token '" + tok + "'");
            });

            validateStringContainsRequiredTokens(brokerAddressPatternPart, REQUIRED_TOKEN_SET, tok -> {
                throw new IllegalArgumentException("advertisedBrokerAddressPattern must contain at least one nodeId replacement pattern '" + tok + "'");
            });
            this.parsedBootstrapAddressPattern = bootstrapPatternAndPort.addressPattern();
            this.parsedBrokerAddressPattern = brokerAddressPatternPart;
        }

        public String getBootstrapAddressPattern() {
            return parsedBootstrapAddressPattern;
        }

        public String getAdvertisedBrokerAddressPattern() {
            return advertisedBrokerAddressPattern;
        }

        public int getAdvertisedPort() {
            return advertisedPort;
        }

        public int getBootstrapPort() {
            return bootstrapPort;
        }

    }

}
