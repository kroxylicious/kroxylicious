/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper.fixedcluster;

import java.net.InetSocketAddress;

import io.kroxylicious.proxy.addressmapper.AddressMapper;

public class FixedClusterAddressMapper implements AddressMapper {
    private String bootstrapServers;

    public FixedClusterAddressMapper(String bootstrapServers) {

        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public InetSocketAddress getDownstream(String upstreamHost, int upstreamPort) {
        return null;
    }

    @Override
    public InetSocketAddress getUpstream() {
        String[] split = bootstrapServers.split(":", 2);
        return InetSocketAddress.createUnresolved(split[0], Integer.parseInt(split[1]));
    }
}
