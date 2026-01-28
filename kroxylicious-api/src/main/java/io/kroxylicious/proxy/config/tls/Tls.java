/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.Locale;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Provides TLS configuration for this peer.  This class is designed to be used for both TLS server and client roles.
 *
 * @param key specifies a key provider that provides the certificate/key used to identify this peer.
 * @param trust specifies a trust provider used by this peer to determine whether to trust the peer. If omitted platform trust is used instead.
 * @param cipherSuites specifies a custom object which contains details of allowed and denied cipher suites
 * @param protocols specifies a custom object which contains details of allowed and denied tls protocols
 * @param tlsCredentialSupplier specifies a dynamic TLS credential supplier for per-client certificate selection (optional)
 */
public record Tls(@Nullable KeyProvider key,
                  @Nullable TrustProvider trust,
                  @Nullable AllowDeny<String> cipherSuites,
                  @Nullable AllowDeny<String> protocols,
                  @Nullable TlsCredentialSupplierDefinition tlsCredentialSupplier) {

    /**
     * Creates a Tls configuration without a TLS credential supplier.
     * This constructor is provided for backward compatibility with v0.18.0.
     * <p>
     * For configurations that require dynamic TLS credential selection, use the
     * 5-parameter constructor that includes {@code tlsCredentialSupplier}.
     *
     * @param key specifies a key provider that provides the certificate/key used to identify this peer.
     * @param trust specifies a trust provider used by this peer to determine whether to trust the peer.
     * @param cipherSuites specifies allowed and denied cipher suites
     * @param protocols specifies allowed and denied tls protocols
     */
    public Tls(@Nullable KeyProvider key,
               @Nullable TrustProvider trust,
               @Nullable AllowDeny<String> cipherSuites,
               @Nullable AllowDeny<String> protocols) {
        this(key, trust, cipherSuites, protocols, null);
    }

    public static final String PEM = "PEM";

    public static String getStoreTypeOrPlatformDefault(@Nullable String storeType) {
        return storeType == null ? KeyStore.getDefaultType().toUpperCase(Locale.ROOT) : storeType.toUpperCase(Locale.ROOT);
    }

    public boolean definesKey() {
        return key != null;
    }

}
