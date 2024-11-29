/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

public enum TlsClientAuth {

    REQUIRED("required", "REQUIRE"),
    REQUESTED("requested", "OPTIONAL"),
    NONE("none", "NONE");

    private final String clientAuth;
    private final String nettyClientAuth;

    TlsClientAuth(String clientAuth, String nettyClientAuth) {
        this.clientAuth = clientAuth;
        this.nettyClientAuth = nettyClientAuth;
    }
}
