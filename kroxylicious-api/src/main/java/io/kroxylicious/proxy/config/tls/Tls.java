/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Provides TLS configuration for this peer.  This class is designed to be used for both TLS server and client roles.
 *
 * @param key specifies a key provider that provides the certificate/key used to identify this peer.
 * @param trust specifies a trust provider used by this peer to determine whether to trust the peer. If omitted platform trust is used instead.
 * @param cipherSuites specifies a custom object which contains details of allowed and denied cipher suites
 * @param protocols specifies a custom object which contains details of allowed and denied tls protocols
 * @param credentialSupplier specifies a dynamic TLS credential supplier for per-client certificate selection (optional)
 * @param namedGroups specifies a custom object which contains details of allowed and denied named groups
 */
public record Tls(@Nullable KeyProvider key,
                  @Nullable TrustProvider trust,
                  @Nullable AllowDeny<String> cipherSuites,
                  @Nullable AllowDeny<String> protocols,
                  @Nullable TlsCredentialSupplierConfig credentialSupplier,
                  @Nullable AllowDeny<String> namedGroups) {

    /**
     * Compact constructor with validation.
     */
    public Tls {
        if (key != null && credentialSupplier != null) {
            throw new IllegalArgumentException(
                    "Cannot configure both 'key' and 'credentialSupplier' - they are mutually exclusive. " +
                            "Use 'key' for static TLS credentials or 'credentialSupplier' for dynamic credential selection.");
        }
    }

    /**
     * Creates a Tls configuration without a TLS credential supplier.
     * This constructor is provided for backward compatibility with v0.18.0.
     * <p>
     * For configurations that require dynamic TLS credential selection, use the
     * 5-parameter constructor that includes {@code credentialSupplier}.
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
        this(key, trust, cipherSuites, protocols, null, null);
    }

    /**
     * Creates a Tls configuration without named groups.
     * This constructor is provided for backward compatibility with v0.25.0.
     * <p>
     * For configurations that require dynamic named groups, use the
     * 6-parameter constructor that includes {@code namedGroups}.
     *
     * @param key specifies a key provider that provides the certificate/key used to identify this peer.
     * @param trust specifies a trust provider used by this peer to determine whether to trust the peer.
     * @param cipherSuites specifies allowed and denied cipher suites
     * @param protocols specifies allowed and denied tls protocols
     * @param credentialSupplier specifies a dynamic TLS credential supplier for per-client certificate selection
     */
    public Tls(@Nullable KeyProvider key,
               @Nullable TrustProvider trust,
               @Nullable AllowDeny<String> cipherSuites,
               @Nullable AllowDeny<String> protocols,
               @Nullable TlsCredentialSupplierConfig credentialSupplier) {
        this(key, trust, cipherSuites, protocols, credentialSupplier, null);
    }

    public static final String PEM = "PEM";

    /**
     * Returns the store type, defaulting to the platform default if null.
     * @param storeType the store type, or null for platform default
     * @return the resolved store type in upper case
     */
    public static String getStoreTypeOrPlatformDefault(@Nullable String storeType) {
        return storeType == null ? KeyStore.getDefaultType().toUpperCase(Locale.ROOT) : storeType.toUpperCase(Locale.ROOT);
    }

    /**
     * Validates a named groups configuration, failing fast when it cannot be satisfied by the running JDK.
     * A configuration that specifies only a denied list relies on pruning the JDK's default named groups. Those
     * defaults are not exposed by every JDK provider or version, so such a configuration is rejected when they are
     * unavailable rather than silently leaving the denied groups enabled.
     *
     * @throws IllegalStateException if the configuration cannot be satisfied by the running JDK.
     */
    public void validateNamedGroups() throws IllegalStateException {
        final boolean hasAllowed = Optional.ofNullable(namedGroups).map(AllowDeny::allowed).map(allowed -> !allowed.isEmpty()).orElse(false);
        final boolean hasDenied = Optional.ofNullable(namedGroups).map(AllowDeny::denied).map(denied -> !denied.isEmpty()).orElse(false);
        if (hasDenied && !hasAllowed) {
            try {
                final var defaultNames = SSLContext.getDefault().getDefaultSSLParameters().getNamedGroups();
                if (Objects.isNull(defaultNames) || defaultNames.length == 0) {
                    throw new IllegalStateException("'namedGroups' specifies only a 'denied' list, but this platform does not expose its default named " +
                            "groups, so the denied groups cannot be applied. Specify an 'allowed' list instead.");
                }
            }
            catch (final NoSuchAlgorithmException e) {
                throw new IllegalStateException("Unable to obtain the platform default SSL parameters", e);
            }
        }
    }

    /**
     * Returns whether a key provider is configured.
     * @return true if a key provider is set
     */
    public boolean definesKey() {
        return key != null;
    }

}
