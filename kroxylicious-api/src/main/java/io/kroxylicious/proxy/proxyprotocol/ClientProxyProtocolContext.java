/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.proxyprotocol;

import java.net.SocketAddress;

import io.kroxylicious.proxy.filter.FilterContext;

/**
 * Exposes PROXY protocol information about the client-to-proxy connection to plugins, for example using
 * {@link FilterContext#clientProxyProtocolContext()}. This is implemented by the runtime for use by plugins.
 * <p>
 * For the TCP connections currently supported by Kroxylicious, the addresses returned by this interface are
 * {@link java.net.InetSocketAddress} instances carrying the IP address and port. The {@code getHostName()}
 * caveat documented on {@link FilterContext#clientAddress()} applies to them too.
 * <p>
 * <strong>These are unauthenticated values claimed by the immediate peer.</strong> Kroxylicious does not
 * verify that they reflect the true origin of the connection. Treat them as untrusted unless the immediate
 * peer is a trusted PROXY protocol sender and untrusted clients cannot reach the listener directly. See
 * {@link FilterContext#clientAddress()} for the full trust discussion.
 */
public interface ClientProxyProtocolContext {

    /**
     * The original client (source) address reported by a TCP4 or TCP6 PROXY protocol header.
     * @return the original client source address
     */
    SocketAddress sourceAddress();

    /**
     * The destination address the client connected to, as reported by a TCP4 or TCP6 PROXY protocol header.
     * @return the destination address
     */
    SocketAddress destinationAddress();
}
