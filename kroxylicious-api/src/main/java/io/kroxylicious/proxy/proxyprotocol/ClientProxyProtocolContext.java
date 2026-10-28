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
 */
public interface ClientProxyProtocolContext {

    /**
     * The original client (source) address reported by a TCP4 or TCP6 PROXY protocol header. The
     * returned {@link java.net.InetSocketAddress} includes the IP address and port. The
     * {@code getHostName()} caveat on {@link FilterContext#clientAddress()} applies to this address too.
     * <p>
     * <strong>This is an unauthenticated value claimed by the immediate peer.</strong> Kroxylicious
     * does not verify that it reflects the true origin of the connection; treat it as untrusted unless
     * the immediate peer is a trusted PROXY protocol sender and untrusted clients cannot reach the
     * listener directly. See {@link FilterContext#clientAddress()} for the full trust discussion.
     * @return the original client source address
     */
    SocketAddress sourceAddress();

    /**
     * The destination address the client connected to, as reported by a TCP4 or TCP6 PROXY protocol
     * header. The returned {@link java.net.InetSocketAddress} includes the IP address and port. The
     * {@code getHostName()} caveat on {@link FilterContext#clientAddress()} applies to this address too.
     * @return the destination address
     */
    SocketAddress destinationAddress();
}
