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
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;

import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.Nullable;

public class NettyKeyProvider {

    interface SslContextBuilderA {
        SslContextBuilder keyManager(File keyCertChainFile, File keyFile, @Nullable String keyPassword);
    }

    interface SslContextBuilderB {
        SslContextBuilder keyManager(KeyManagerFactory keyManagerFactory);
    }

    private final KeyProvider delegate;

    public NettyKeyProvider(KeyProvider delegate) {
        this.delegate = delegate;
    }

    public SslContextBuilder forClient() {
        SslContextBuilder client = SslContextBuilder.forClient();
        return configureBuilder(client::keyManager, client::keyManager);
    }

    public SslContextBuilder forServer() {
        return configureBuilder(SslContextBuilder::forServer, SslContextBuilder::forServer);
    }

    private SslContextBuilder configureBuilder(SslContextBuilderA a, SslContextBuilderB b) {
        return this.delegate.accept(new KeyProviderVisitor<>() {
            @Override
            public SslContextBuilder visit(KeyPair keyPair) {
                try {
                    return a.keyManager(new File(keyPair.certificateFile()), new File(keyPair.privateKeyFile()),
                            Optional.ofNullable(keyPair.keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).orElse(null));
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for KeyPair: " + keyPair, e);
                }
            }

            @Override
            public SslContextBuilder visit(KeyStore keyStore) {
                try {
                    var keyStoreFile = new File(keyStore.storeFile());

                    if (keyStore.isPemType()) {
                        return a.keyManager(keyStoreFile, keyStoreFile,
                                Optional.ofNullable(keyStore.keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).orElse(null));
                    }
                    else {
                        return b.keyManager(keyManagerFactory(keyStore));
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for KeyStore: " + keyStore, e);
                }
            }
        });
    }

    private KeyManagerFactory keyManagerFactory(KeyStore store) {
        try (var is = new FileInputStream(store.storeFile())) {
            var password = Optional.ofNullable(store.storePasswordProvider()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
            var keyStore = java.security.KeyStore.getInstance(store.getType());
            keyStore.load(is, password);
            var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore,
                    Optional.ofNullable(store.keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(password));
            return keyManagerFactory;
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Error building SSLContext from : " + store.storeFile(), e);
        }
    }

}
