/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link KeyProvider} backed by a Java Truststore.
 *
 * @param storeFile             location of a key store, or reference to a PEM file containing both private-key and certificate/intermediates.
 * @param storePasswordProvider provider for the store password or null if store does not require a password.
 * @param keyPasswordProvider   provider for the key password. if null the password obtained from the storePasswordProvider will be used to decrypt the key.
 * @param storeType             specifies the server key type. Legal values are those types supported by the platform {@link java.security.KeyStore},
 *                              and PEM (for X-509 certificates express in PEM format).
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "The paths provide the location for key material which may exist anywhere on the file-system. Paths are provided by the user in the administrator role via Kroxylicious configuration. ")
public record KeyStore(@JsonProperty(required = true) String storeFile,
                       @JsonProperty(value = "storePassword") @Nullable PasswordProvider storePasswordProvider,
                       @JsonProperty(value = "keyPassword") @Nullable PasswordProvider keyPasswordProvider,
                       String storeType)
        implements KeyProvider {

    public KeyStore {
        Objects.requireNonNull(storeFile);
    }

    public String getType() {
        return Tls.getStoreTypeOrPlatformDefault(storeType);
    }

    public boolean isPemType() {
        return Objects.equals(getType(), Tls.PEM);
    }

    @Override
    public <T> T accept(KeyProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
