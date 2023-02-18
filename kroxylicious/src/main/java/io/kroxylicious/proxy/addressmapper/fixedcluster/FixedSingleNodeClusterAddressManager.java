/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper.fixedcluster;

import io.kroxylicious.proxy.addressmapper.AddressManager;
import io.kroxylicious.proxy.addressmapper.AddressMapper;
import io.kroxylicious.proxy.config.ProxyConfig;
import io.kroxylicious.proxy.filter.NetFilterContext;

/**
 * Simple address manager that supports a single cluster that comprises a single broker.
 */
public class FixedSingleNodeClusterAddressManager implements AddressManager {
    private final ProxyConfig proxyConfig;
    private final FixedSingleNodeClusterAddressManagerConfig config;

    public FixedSingleNodeClusterAddressManager(ProxyConfig proxyConfig, FixedSingleNodeClusterAddressManagerConfig config) {
        this.proxyConfig = proxyConfig;
        this.config = config;
    }

    @Override
    public AddressMapper createMapper(NetFilterContext context) {
        return new FixedSingleNodeClusterAddressMapper(proxyConfig.address(), config.bootstrapServers());
    }
}
