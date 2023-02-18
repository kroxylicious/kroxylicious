/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper.fixedcluster;

import java.net.InetSocketAddress;

import io.kroxylicious.proxy.addressmapper.AddressMapper;

public class FixedSingleNodeClusterAddressMapper implements AddressMapper {
    private final InetSocketAddress upstream;
    private final InetSocketAddress downstream;

    public FixedSingleNodeClusterAddressMapper(String downstreamAddress, String upstreamAddress) {
        this.downstream = buildAddress(downstreamAddress);
        this.upstream = buildAddress(upstreamAddress);
    }

    @Override
    public InetSocketAddress getDownstream(String upstreamHost, int upstreamPort) {
        return downstream;
    }

    @Override
    public InetSocketAddress getUpstream() {
        return upstream;
    }

    private static InetSocketAddress buildAddress(String address) {
        var parts = address.split(":", 2);
        return InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1]));
    }
}
