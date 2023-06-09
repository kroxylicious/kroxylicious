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
import java.security.KeyStore;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.SslContextBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link TrustProvider} backed by a Java Truststore.
 *
 * @param storeFile     location of a key store, or reference to a PEM file containing both private-key/certificate/intermediates.
 * @param storePassword password used to protect the key store.
 * @param storeType     specifies the server key type. Legal values are those types supported by the platform {@link KeyStore},
 *                      and PEM (for X-509 certificates express in PEM format).
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Requires ability to consume file resources from arbitrary, user-specified, locations on the file-system.")
public record TrustStore(String storeFile,
                         PasswordProvider storePassword,
                         String storeType) implements TrustProvider {

    public String getType() {
        return Tls.getStoreTypeOrPlatformDefault(storeType);
    }

    public boolean isPemType() {
        return Objects.equals(getType(), Tls.PEM);
    }

    @Override
    public void apply(SslContextBuilder sslContextBuilder) {
        var trustStore = new File(storeFile());
        if (isPemType()) {
            sslContextBuilder.trustManager(trustStore);
        }
        else {
            try (var is = new FileInputStream(trustStore)) {
                var password = Optional.ofNullable(this.storePassword()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
                var keyStore = KeyStore.getInstance(this.getType());
                keyStore.load(is, password);

                var trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(keyStore);
                sslContextBuilder.trustManager(trustManagerFactory);
            }
            catch (GeneralSecurityException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
