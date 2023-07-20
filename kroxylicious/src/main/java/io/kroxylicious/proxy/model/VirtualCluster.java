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

public class VirtualCluster implements ClusterNetworkAddressConfigProvider {

    private final TargetCluster targetCluster;

    private final Optional<Tls> tls;
    private final boolean logNetwork;

    private final boolean logFrames;

    private final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider;
    private final Optional<SslContext> upstreamSslContext;
    private final Optional<SslContext> downstreamSslContext;

    public VirtualCluster(TargetCluster targetCluster,
                          ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider,
                          Optional<Tls> tls,
                          boolean logNetwork, boolean logFrames) {
        this.tls = tls;
        if (clusterNetworkAddressConfigProvider.requiresTls() && (tls.isEmpty() || !tls.get().definesKey())) {
            throw new IllegalStateException("Cluster endpoint provider requires server TLS, but this virtual cluster does not define it.");
        }
        var conflicts = clusterNetworkAddressConfigProvider.getExclusivePorts().stream().filter(p -> clusterNetworkAddressConfigProvider.getSharedPorts().contains(p))
                .collect(Collectors.toSet());
        if (!conflicts.isEmpty()) {
            throw new IllegalStateException(
                    "The set of exclusive ports described by the cluster endpoint provider must be distinct from those described as shared. Intersection: " + conflicts);
        }
        this.targetCluster = targetCluster;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.clusterNetworkAddressConfigProvider = clusterNetworkAddressConfigProvider;

        // TODO: https://github.com/kroxylicious/kroxylicious/issues/104 be prepared to reload the SslContext at runtime.
        this.upstreamSslContext = buildUpstreamSslContext();
        this.downstreamSslContext = buildDownstreamSslContext();
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VirtualCluster [");
        sb.append("targetCluster=").append(targetCluster);
        sb.append(", clusterNetworkAddressConfigProvider=").append(clusterNetworkAddressConfigProvider);
        sb.append(", tls=").append(tls.map(Tls::toString).orElse(null));
        sb.append(", logNetwork=").append(logNetwork);
        sb.append(", logFrames=").append(logFrames);
        sb.append(']');
        return sb.toString();
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

}
