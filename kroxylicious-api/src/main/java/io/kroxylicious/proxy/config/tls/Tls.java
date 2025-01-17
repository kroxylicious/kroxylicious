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
 * @param key specifies a key provider that provides the certificate/key used to identify this peer.
 * @param trust specifies a trust provider used by this peer to determine whether to trust the peer. If omitted platform trust is used instead.
 * @param cipherSuites specifies a custom object which contains details of allowed and denied cipher suites
 * @param protocols specifies a custom object which contains details of allowed and denied tls protocols
 */
public record Tls(KeyProvider key,
                  TrustProvider trust,
                  AllowDeny<String> cipherSuites,
                  AllowDeny<String> protocols) {

    // Sundrio seems to need constructor order to matter, or it won't generate the with* methods for integration tests
    // This can be removed once the old constructor is deprecated as unnecessary when the record will be generating it
    public Tls(KeyProvider key, TrustProvider trust, AllowDeny<String> cipherSuites, AllowDeny<String> protocols) {
        this.key = key;
        this.trust = trust;
        this.cipherSuites = cipherSuites;
        this.protocols = protocols;
    }

    /**
     * @deprecated use the all args constructor
     * {@see io.kroxylicious.proxy.config.tls.Tls#Tls(
     *      io.kroxylicious.proxy.config.tls.KeyProvider,
     *      io.kroxylicious.proxy.config.tls.TrustProvider,
     *      io.kroxylicious.proxy.config.tls.AllowDeny<String>,
     *      io.kroxylicious.proxy.config.tls.AllowDeny<String>)
     * }
     */
    // This is required for API backwards compatability
    @Deprecated(forRemoval = true, since = "0.10.0")
    public Tls(KeyProvider key, TrustProvider trust) {
        this(key, trust, null, null);
    }

    public static final String PEM = "PEM";

    public static String getStoreTypeOrPlatformDefault(String storeType) {
        return storeType == null ? KeyStore.getDefaultType().toUpperCase(Locale.ROOT) : storeType.toUpperCase(Locale.ROOT);
    }

    public boolean definesKey() {
        return key != null;
    }

}
