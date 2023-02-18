/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.multitenant;

import io.kroxylicious.proxy.addressmapper.AddressManager;
import io.kroxylicious.proxy.addressmapper.AddressMapper;
import io.kroxylicious.proxy.config.ProxyConfig;
import io.kroxylicious.proxy.filter.NetFilterContext;

/**
 * Test implementation of Address Manager that supports a single backend cluster with a single node.  Downstream
 * address addresses are mapped using the SNI name used by the client to connect to the upstream.
 */
public class FixedClusterSingleNodeSniAddressManager implements AddressManager {
    private final ProxyConfig proxyConfig;
    private final FixedClusterSingleNodeSniAddressManagerConfig config;

    public FixedClusterSingleNodeSniAddressManager(ProxyConfig proxyConfig, FixedClusterSingleNodeSniAddressManagerConfig config) {
        this.proxyConfig = proxyConfig;
        this.config = config;
    }

    @Override
    public AddressMapper createMapper(NetFilterContext context) {
        return new FixedClusterSingleNodeSniAddressMapper(proxyConfig.address(), config.bootstrapServers(), context.sniHostname());
    }
}
