/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper;

import java.net.InetSocketAddress;

/**
 * Address
 */
public interface AddressMapper {

    /**
     * Gets the downstream for the corresponding upstream.
     *
     * @param upstreamHost upstream host
     * @param upstreamPort upstream port
     * @return unresolved InetSocketAddress providing the downstream host/port.  The address may be unresolved.
     */
    InetSocketAddress getDownstream(String upstreamHost, int upstreamPort);

    InetSocketAddress getUpstream();

}
