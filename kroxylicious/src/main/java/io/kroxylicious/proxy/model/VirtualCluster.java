/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.model;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.KeyProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class VirtualCluster implements ClusterNetworkAddressConfigProvider {
    private final String clusterName;

    private final TargetCluster targetCluster;

    private final Optional<Tls> tls;
    private final boolean logNetwork;

    private final boolean logFrames;

    private final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider;

    private final Optional<SslContext> upstreamSslContext;

    private final Optional<SslContext> downstreamSslContext;

    public VirtualCluster(String clusterName,
                          TargetCluster targetCluster,
                          ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider,
                          Optional<Tls> tls,
                          boolean logNetwork,
                          boolean logFrames) {
        this.clusterName = clusterName;
        this.tls = tls;
        this.targetCluster = targetCluster;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.clusterNetworkAddressConfigProvider = clusterNetworkAddressConfigProvider;

        validateTLsSettings(clusterNetworkAddressConfigProvider, tls);
        validatePortUsage(clusterNetworkAddressConfigProvider);

        // TODO: https://github.com/kroxylicious/kroxylicious/issues/104 be prepared to reload the SslContext at runtime.
        this.upstreamSslContext = buildUpstreamSslContext();
        this.downstreamSslContext = buildDownstreamSslContext();
    }

    public String getClusterName() {
        return clusterName;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public ClusterNetworkAddressConfigProvider getClusterNetworkAddressConfigProvider() {
        return clusterNetworkAddressConfigProvider;
    }

    public boolean isLogNetwork() {
        return logNetwork;
    }

    public boolean isLogFrames() {
        return logFrames;
    }

    public boolean isUseTls() {
        return tls.isPresent();
    }

    @Override
    public String toString() {
        return "VirtualCluster{" +
                "clusterName='" + clusterName + '\'' +
                ", targetCluster=" + targetCluster +
                ", tls=" + tls +
                ", logNetwork=" + logNetwork +
                ", logFrames=" + logFrames +
                ", clusterNetworkAddressConfigProvider=" + clusterNetworkAddressConfigProvider +
                ", upstreamSslContext=" + upstreamSslContext +
                ", downstreamSslContext=" + downstreamSslContext +
                '}';
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return clusterNetworkAddressConfigProvider.getClusterBootstrapAddress();
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        return clusterNetworkAddressConfigProvider.getBrokerAddress(nodeId);
    }

    @Override
    public Optional<String> getBindAddress() {
        return clusterNetworkAddressConfigProvider.getBindAddress();
    }

    @Override
    public boolean requiresTls() {
        return clusterNetworkAddressConfigProvider.requiresTls();
    }

    @Override
    public Set<Integer> getExclusivePorts() {
        return clusterNetworkAddressConfigProvider.getExclusivePorts();
    }

    @Override
    public Set<Integer> getSharedPorts() {
        return clusterNetworkAddressConfigProvider.getSharedPorts();
    }

    @Override
    public Map<Integer, HostPort> discoveryAddressMap() {
        return clusterNetworkAddressConfigProvider.discoveryAddressMap();
    }

    @Override
    public Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
        return clusterNetworkAddressConfigProvider.getBrokerIdFromBrokerAddress(brokerAddress);
    }

    public Optional<SslContext> getDownstreamSslContext() {
        return downstreamSslContext;
    }

    public Optional<SslContext> getUpstreamSslContext() {
        return upstreamSslContext;
    }

    private Optional<SslContext> buildDownstreamSslContext() {
        return tls.map(tls -> {
            try {
                return Optional.of(tls.key()).map(KeyProvider::forServer).orElseThrow().build();
            }
            catch (SSLException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Optional<SslContext> buildUpstreamSslContext() {
        return targetCluster.tls().map(tls -> {
            try {
                var sslContextBuilder = SslContextBuilder.forClient();
                Optional.ofNullable(tls.trust()).ifPresent(tp -> tp.apply(sslContextBuilder));
                return sslContextBuilder.build();
            }
            catch (SSLException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static void validatePortUsage(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider) {
        var conflicts = clusterNetworkAddressConfigProvider.getExclusivePorts().stream().filter(p -> clusterNetworkAddressConfigProvider.getSharedPorts().contains(p))
                .collect(Collectors.toSet());
        if (!conflicts.isEmpty()) {
            throw new IllegalStateException(
                    "The set of exclusive ports described by the cluster endpoint provider must be distinct from those described as shared. Intersection: " + conflicts);
        }
    }

    private static void validateTLsSettings(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider, Optional<Tls> tls) {
        if (clusterNetworkAddressConfigProvider.requiresTls() && (tls.isEmpty() || !tls.get().definesKey())) {
            throw new IllegalStateException("Cluster endpoint provider requires server TLS, but this virtual cluster does not define it.");
        }
    }
}
