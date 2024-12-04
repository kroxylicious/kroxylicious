/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link TrustProvider} backed by a Java Truststore.
 *
 * @param storeFile location of a key store, or reference to a PEM file containing both private-key/certificate/intermediates.
 * @param storePasswordProvider provider for the store password or null if store does not require a password.
 * @param storeType specifies the server key type. Legal values are those types supported by the platform {@link KeyStore},
 *         and PEM (for X-509 certificates express in PEM format).
 * @param trustOptions the trust options that will be applied to this peer.
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "The paths provide the location for key material which may exist anywhere on the file-system. Paths are provided by the user in the administrator role via Kroxylicious configuration. ")
public record TrustStore(@JsonProperty(required = true) String storeFile,
                         @JsonProperty(value = "storePassword") PasswordProvider storePasswordProvider,
                         String storeType,
                         @Nullable @JsonProperty(value = "trustOptions") TrustOptions trustOptions)
        implements TrustProvider {

    public TrustStore {
        Objects.requireNonNull(storeFile);
    }

    /**
     * A {@link TrustProvider} backed by a Java Truststore.
     *
     * @param storeFile location of a key store, or reference to a PEM file containing both private-key/certificate/intermediates.
     * @param storePasswordProvider provider for the store password or null if store does not require a password.
     * @param storeType specifies the server key type. Legal values are those types supported by the platform {@link KeyStore},
     *         and PEM (for X-509 certificates express in PEM format).
     */
    public TrustStore(String storeFile, PasswordProvider storePasswordProvider, String storeType) {
        this(storeFile, storePasswordProvider, storeType, null);
    }

    public String getType() {
        return Tls.getStoreTypeOrPlatformDefault(storeType);
    }

    public boolean isPemType() {
        return Objects.equals(getType(), Tls.PEM);
    }

    @Override
    public <T> T accept(TrustProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
