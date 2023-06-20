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

import io.netty.handler.ssl.SslContextBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link KeyProvider} backed by a Java Truststore.
 *
 * @param storeFile     location of a key store, or reference to a PEM file containing both private-key/certificate/intermediates.
 * @param storePassword password used to protect the key store. cannot be used if trustType is PEM.
 * @param keyPassword      password used to protect the key within the storeFile or privateKeyFile
 * @param storeType       specifies the server key type. Legal values are those types supported by the platform {@link java.security.KeyStore},
 *                         and PEM (for X-509 certificates express in PEM format).
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Requires ability to consume file resources from arbitrary, user-specified, locations on the file-system.")
public record KeyStore(String storeFile,
                       PasswordProvider storePassword,
                       PasswordProvider keyPassword,
                       String storeType) implements KeyProvider {

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
                    Optional.ofNullable(keyPassword()).map(PasswordProvider::getProvidedPassword).orElse(null));
        }
        else {
            try (var is = new FileInputStream(keyStoreFile)) {
                var password = Optional.ofNullable(this.storePassword()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
                var keyStore = java.security.KeyStore.getInstance(this.getType());
                keyStore.load(is, password);
                var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore,
                        Optional.ofNullable(this.keyPassword()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(password));
                return SslContextBuilder.forServer(keyManagerFactory);
            }
            catch (GeneralSecurityException | IOException e) {
                throw new RuntimeException("Error building SSLContext from : " + keyStoreFile, e);
            }
        }

    }
}
