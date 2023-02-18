/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.multitenant;

import java.net.InetSocketAddress;

import io.kroxylicious.proxy.addressmapper.AddressMapper;

public class FixedClusterSingleNodeSniAddressMapper implements AddressMapper {
    private final InetSocketAddress upstream;
    private final InetSocketAddress downstream;

    public FixedClusterSingleNodeSniAddressMapper(String downstreamAddress, String upstreamAddress, String sniHostname) {
        var downstreamParts = downstreamAddress.split(":", 2);
        this.downstream = InetSocketAddress.createUnresolved(sniHostname == null ? downstreamParts[0] : sniHostname, Integer.parseInt(downstreamParts[1]));

        var upstreamParts = upstreamAddress.split(":", 2);
        this.upstream = InetSocketAddress.createUnresolved(upstreamParts[0], Integer.parseInt(upstreamParts[1]));
    }

    @Override
    public InetSocketAddress getDownstream(String upstreamHost, int upstreamPort) {
        return downstream;
    }

    @Override
    public InetSocketAddress getUpstream() {
        return upstream;
    }

}
