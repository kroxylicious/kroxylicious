/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.Locale;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Provides TLS configuration for this peer.  This class is designed to be used for both TLS server and client roles.
 *
 * @param key   specifies a key provider that provides the certificate/key used to identify this peer.
 * @param trust specifies a trust provider used by this peer to determine whether to trust the peer. If omitted platform trust is used instead.
 * @param clientAuth specifies how the TLS session should authenticate clients. Optional not required when configuring upstream TLS
 *
 * TODO ability to restrict by TLS protocol and cipher suite.
 */
public record Tls(KeyProvider key,
                  TrustProvider trust,
                  @Nullable TlsClientAuth clientAuth) {

    // Sonar is wrong about this being equivalent to the default ctor as the fields are not initialised by the default before calling the validation.
    @SuppressWarnings("java:S6207")
    public Tls(KeyProvider key, TrustProvider trust, TlsClientAuth clientAuth) {
        this.key = key;
        this.trust = trust;
        this.clientAuth = clientAuth;
        validateClientAuth();
    }

    /**
     * @deprecated use the all args constructor {@see io.kroxylicious.proxy.config.tls.Tls#Tls(io.kroxylicious.proxy.config.tls.KeyProvider, io.kroxylicious.proxy.config.tls.TrustProvider, io.kroxylicious.proxy.config.tls.TlsClientAuth)}
     */
    // This is required for API backwards compatability
    @Deprecated(forRemoval = true, since = "0.10.0")
    public Tls(KeyProvider key, TrustProvider trust) {
        this(key, trust, null);
    }

    public static final String PEM = "PEM";

    public static String getStoreTypeOrPlatformDefault(String storeType) {
        return storeType == null ? KeyStore.getDefaultType().toUpperCase(Locale.ROOT) : storeType.toUpperCase(Locale.ROOT);
    }

    public boolean definesKey() {
        return key != null;
    }

    @Override
    public TlsClientAuth clientAuth() {
        // We can't just default this in the canonical constructor as it then becomes a nonsensical property of Upstream TLS config
        return clientAuth == null ? TlsClientAuth.NONE : clientAuth;
    }

    private void validateClientAuth() {
        if (clientAuth() != TlsClientAuth.NONE && Objects.isNull(trust)) {
            throw new IllegalStateException("ClientAuth enabled but no TrustStore provided to validate certificates");
        }
    }
}
