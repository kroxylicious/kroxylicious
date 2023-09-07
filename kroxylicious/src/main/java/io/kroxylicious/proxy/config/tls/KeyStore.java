/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.netty.handler.ssl.SslContextBuilder;

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
public record KeyStore(String storeFile,
                       @JsonProperty(value = "storePassword") PasswordProvider storePasswordProvider,
                       @JsonProperty(value = "keyPassword") PasswordProvider keyPasswordProvider,
                       String storeType)
        implements KeyProvider {

    public String getType() {
        return Tls.getStoreTypeOrPlatformDefault(storeType);
    }

    public boolean isPemType() {
        return Objects.equals(getType(), Tls.PEM);
    }

    @Override
    public SslContextBuilder forServer() {

        var keyStoreFile = new File(storeFile);
        if (isPemType()) {
            return SslContextBuilder.forServer(keyStoreFile, keyStoreFile,
                    Optional.ofNullable(keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).orElse(null));
        }
        else {
            try (var is = new FileInputStream(keyStoreFile)) {
                var password = Optional.ofNullable(this.storePasswordProvider()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
                var keyStore = java.security.KeyStore.getInstance(this.getType());
                keyStore.load(is, password);
                var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore,
                        Optional.ofNullable(this.keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(password));
                return SslContextBuilder.forServer(keyManagerFactory);
            }
            catch (GeneralSecurityException | IOException e) {
                throw new RuntimeException("Error building SSLContext from : " + keyStoreFile, e);
            }
        }

    }
}
