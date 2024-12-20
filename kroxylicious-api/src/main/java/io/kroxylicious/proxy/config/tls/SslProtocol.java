/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.util.Arrays;
import java.util.Optional;

public enum SslProtocol {
    SSLv2("SSLv2Hello"),
    SSLv3("SSLv3"),
    TLSv1("TLSv1"),
    TLSv1_1("TLSv1.1"),
    TLSv1_2("TLSv1.2"),
    TLSv1_3("TLSv1.3");

    String sslProtocol;

    SslProtocol(String sslProtocol) {
        this.sslProtocol = sslProtocol;
    }

    public String getSslProtocol() {
        return sslProtocol;
    }

    public static Optional<SslProtocol> getProtocolName(String sslProtocol) {
        return Arrays.stream(SslProtocol.values())
                .filter(protocol -> protocol.getSslProtocol().equals(sslProtocol))
                .findFirst();
    }
}
