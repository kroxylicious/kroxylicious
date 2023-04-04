/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Optional;

public class VirtualCluster {

    private final TargetCluster targetCluster;
    private final ClusterEndpointProviderDefinition clusterEndpointProvider;

    private final Optional<String> keyStoreFile;
    private final Optional<String> keyPassword;

    private final boolean logNetwork;

    private final boolean logFrames;
    private final boolean useIoUring;

    public VirtualCluster(TargetCluster targetCluster, ClusterEndpointProviderDefinition clusterEndpointProvider, Optional<String> keyStoreFile,
                          Optional<String> keyPassword,
                          boolean logNetwork, boolean logFrames, boolean useIoUring) {
        this.targetCluster = targetCluster;
        this.clusterEndpointProvider = clusterEndpointProvider;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.useIoUring = useIoUring;
        this.keyStoreFile = keyStoreFile;
        this.keyPassword = keyPassword;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public ClusterEndpointProviderDefinition clusterEndpointProvider() {
        return clusterEndpointProvider;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VirtualCluster [");
        sb.append("targetCluster=").append(targetCluster);
        sb.append(", clusterEndpointProvider=").append(clusterEndpointProvider);
        sb.append(", keyStoreFile=").append(keyStoreFile);
        sb.append(", keyPassword=").append(keyPassword);
        sb.append(", logNetwork=").append(logNetwork);
        sb.append(", logFrames=").append(logFrames);
        sb.append(", useIoUring=").append(useIoUring);
        sb.append(']');
        return sb.toString();
    }

}
