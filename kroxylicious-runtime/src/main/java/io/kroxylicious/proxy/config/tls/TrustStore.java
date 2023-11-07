/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.netty.handler.ssl.SslContextBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link TrustProvider} backed by a Java Truststore.
 *
 * @param storeFile             location of a key store, or reference to a PEM file containing both private-key/certificate/intermediates.
 * @param storePasswordProvider provider for the store password or null if store does not require a password.
 * @param storeType             specifies the server key type. Legal values are those types supported by the platform {@link KeyStore},
 *                              and PEM (for X-509 certificates express in PEM format).
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "The paths provide the location for key material which may exist anywhere on the file-system. Paths are provided by the user in the administrator role via Kroxylicious configuration. ")
public record TrustStore(String storeFile,
                         @JsonProperty(value = "storePassword") PasswordProvider storePasswordProvider,
                         String storeType)
        implements TrustProvider {

    public String getType() {
        return Tls.getStoreTypeOrPlatformDefault(storeType);
    }

    public boolean isPemType() {
        return Objects.equals(getType(), Tls.PEM);
    }

    @Override
    public void apply(SslContextBuilder sslContextBuilder) {
        var trustStore = new File(storeFile());
        try {
            if (isPemType()) {
                sslContextBuilder.trustManager(trustStore);
            }
            else {
                try (var is = new FileInputStream(trustStore)) {
                    var password = Optional.ofNullable(this.storePasswordProvider()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
                    var keyStore = KeyStore.getInstance(this.getType());
                    keyStore.load(is, password);

                    var trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    trustManagerFactory.init(keyStore);
                    sslContextBuilder.trustManager(trustManagerFactory);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error building SSLContext from : " + trustStore, e);
        }
    }
}
