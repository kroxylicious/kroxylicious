/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.KeyManagerFactory;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public class VirtualCluster {

    private final TargetCluster targetCluster;

    private final Optional<String> keyStoreFile;
    private final Optional<String> keyPassword;

    private final boolean logNetwork;

    private final boolean logFrames;
    private final boolean useIoUring;
    private final ClusterEndpointConfigProvider endpointProvider;
    private final ConcurrentHashMap<Integer, HostPort> upstreamClusterCache = new ConcurrentHashMap<>();

    public VirtualCluster(TargetCluster targetCluster,
                          @JsonDeserialize(converter = ClusterEndpointConfigProviderConverter.class) ClusterEndpointConfigProvider clusterEndpointConfigProvider,
                          Optional<String> keyStoreFile,
                          Optional<String> keyPassword,
                          boolean logNetwork, boolean logFrames, boolean useIoUring) {
        this.targetCluster = targetCluster;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.useIoUring = useIoUring;
        this.keyStoreFile = keyStoreFile;
        this.keyPassword = keyPassword;
        // TODO can we get jackson to instantiate this?
        this.endpointProvider = clusterEndpointConfigProvider;

    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public ClusterEndpointConfigProvider getClusterEndpointProvider() {
        return endpointProvider;
    }

    public Optional<String> keyStoreFile() {
        return keyStoreFile;
    }

    public Optional<String> keyPassword() {
        return keyPassword;
    }

    public boolean isLogNetwork() {
        return logNetwork;
    }

    public boolean isLogFrames() {
        return logFrames;
    }

    public boolean isUseIoUring() {
        return useIoUring;
    }

    public boolean isUseTls() {
        return keyStoreFile.isPresent();
    }

    public Optional<SslContext> buildSslContext() {

        return keyStoreFile.map(ksf -> {
            try (var is = new FileInputStream(ksf)) {
                var password = keyPassword.map(String::toCharArray).orElse(null);
                var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(is, password);
                var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, password);
                return SslContextBuilder.forServer(keyManagerFactory).build();
            }
            catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VirtualCluster [");
        sb.append("targetCluster=").append(targetCluster);
        sb.append(", clusterEndpointProvider=").append(endpointProvider);
        sb.append(", keyStoreFile=").append(keyStoreFile);
        sb.append(", keyPassword=").append(keyPassword);
        sb.append(", logNetwork=").append(logNetwork);
        sb.append(", logFrames=").append(logFrames);
        sb.append(", useIoUring=").append(useIoUring);
        sb.append(']');
        return sb.toString();
    }

    // TODO - better abstraction required around this
    public ConcurrentHashMap<Integer, HostPort> getUpstreamClusterCache() {
        return upstreamClusterCache;
    }

}
