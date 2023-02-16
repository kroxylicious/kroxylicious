/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Optional;

public class ProxyConfig implements ProxiedClusterConfig {

    private final String address;
    private final boolean logNetwork;
    private final boolean logFrames;
    private final boolean useIoUring;
    private final Optional<String> keyStoreFile;
    private final Optional<String> keyPassword;

    public ProxyConfig(String address, boolean logNetwork, boolean logFrames, boolean useIoUring, Optional<String> keyStoreFile, Optional<String> keyPassword) {
        this.address = address;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.useIoUring = useIoUring;
        this.keyStoreFile = keyStoreFile;
        this.keyPassword = keyPassword;
    }

    @Override
    public String address() {
        return address;
    }

    public boolean logNetwork() {
        return logNetwork;
    }

    public boolean logFrames() {
        return logFrames;
    }

    public boolean useIoUring() {
        return useIoUring;
    }

    public Optional<String> keyStoreFile() {
        return keyStoreFile;
    }

    public Optional<String> keyPassword() {
        return keyPassword;
    }
}
