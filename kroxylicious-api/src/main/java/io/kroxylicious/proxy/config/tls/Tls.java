/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.Locale;

/**
 * Provides TLS configuration for this peer.  This class is designed to be used for both TLS server and client roles.
 *
 * @param key   specifies a key provider that provides the certificate/key used to identify this peer.
 * @param trust specifies a trust provider used by this peer to determine whether to trust the peer. If omitted platform trust is used instead.
 *
 * TODO ability to restrict by TLS protocol and cipher suite.
 */
public record Tls(KeyProvider key,
                  TrustProvider trust,
                  TlsClientAuth clientAuth) {
    public Tls(KeyProvider key, TrustProvider trust) {
        this(key, trust, null);
    }

    public Tls(KeyProvider key, TrustProvider trust, TlsClientAuth clientAuth) {
        this.key = key;
        this.trust = trust;
        this.clientAuth = clientAuth == null ? TlsClientAuth.NONE : clientAuth;
    }

    public static final String PEM = "PEM";

    public static String getStoreTypeOrPlatformDefault(String storeType) {
        return storeType == null ? KeyStore.getDefaultType().toUpperCase(Locale.ROOT) : storeType.toUpperCase(Locale.ROOT);
    }

    public boolean definesKey() {
        return key != null;
    }

    public boolean definesClientAuth() {
        return trust != null && clientAuth != null;
    }
}
