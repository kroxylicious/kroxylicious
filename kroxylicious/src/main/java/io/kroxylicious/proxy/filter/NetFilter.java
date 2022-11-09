/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.net.SocketAddress;

/**
 * Abstracts some policy/logic for how an upstream connection for a given client connection
 * is made.
 */
public interface NetFilter {

    /**
     * Determine the upstream cluster to connect to based on the information
     * provided by the given {@code context},
     * by invoking {@link NetFilterContext#connect(String, int, KrpcFilter[])}.
     * @param context The context.
     */
    void upstreamBroker(NetFilterContext context);

    interface NetFilterContext {
        /**
         * @return The address of the client
         * (possibly not the address of the remote TCP/TLS peer, if any intermediate L4 or
         * L7 proxy is propagating client connection information).
         * @see #srcAddress()
         */
        public String clientAddress();

        public int clientPort();

        /**
         * @return The address of the remote peer, which may be a proxy or the ultimate client.
         * @see #clientAddress()
         */
        public SocketAddress srcAddress();

        /**
         * The authorized id, or null if there is no authentication configured for this listener.
         * @return
         */
        public String authorizedId();

        /**
         * @return The name of the client software, if known via ApiVersions request. Otherwise null.
         */
        public String clientSoftwareName();

        /**
         * @return The version of the client software, if known via ApiVersions request. Otherwise null.
         */
        public String clientSoftwareVersion();

        /**
         * @return The <a href="https://en.wikipedia.org/wiki/Server_Name_Indication">SNI</a>
         * hostname which the client used during TLS handshake.
         */
        public String sniHostname();

        /**
         * Connect to the upstream broker at the given {@code host} and {@code port},
         * using the given protocol filters
         * @param host The host
         * @param port The port
         * @param filters The filters
         */
        public void connect(String host, int port, KrpcFilter[] filters);

        // TODO add API for delayed responses
    }
}
