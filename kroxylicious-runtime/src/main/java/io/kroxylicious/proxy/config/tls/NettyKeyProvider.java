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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;

import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public class NettyKeyProvider {

    interface SslContextBuilderA {
        SslContextBuilder keyManager(File keyCertChainFile, File keyFile, String keyPassword);
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
        return this.delegate.accept(new KeyProviderVisitor<>() {
            @Override
            public SslContextBuilder visit(KeyPair keyPair) {
                try {
                    return client.keyManager(new File(keyPair.certificateFile()), new File(keyPair.privateKeyFile()),
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
                        return client.keyManager(keyStoreFile, keyStoreFile,
                                Optional.ofNullable(keyStore.keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).orElse(null));
                    }
                    else {
                        return client.keyManager(keyManagerFactory(keyStore));
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for KeyStore: " + keyStore, e);
                }
            }

            @Override
            public SslContextBuilder visit(KeyPairSet keyPairSet) {
                throw new RuntimeException("KeyPairSet is not supported for client key");
            }
        });
    }

    public SslContextBuilder forServer(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider) {
        return this.delegate.accept(new KeyProviderVisitor<>() {
            @Override
            public SslContextBuilder visit(KeyPair keyPair) {
                try {
                    return SslContextBuilder.forServer(new File(keyPair.certificateFile()), new File(keyPair.privateKeyFile()),
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
                        return SslContextBuilder.forServer(keyStoreFile, keyStoreFile,
                                Optional.ofNullable(keyStore.keyPasswordProvider()).map(PasswordProvider::getProvidedPassword).orElse(null));
                    }
                    else {
                        return ((SslContextBuilderB) SslContextBuilder::forServer).keyManager(keyManagerFactory(keyStore));
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for KeyStore: " + keyStore, e);
                }
            }

            @Override
            public SslContextBuilder visit(KeyPairSet keyPairSet) {
                try {
                    Set<String> hostnames = new HashSet<>();
                    hostnames.add(clusterNetworkAddressConfigProvider.getClusterBootstrapAddress().host());
                    for (HostPort value : clusterNetworkAddressConfigProvider.discoveryAddressMap().values()) {
                        hostnames.add(value.host());
                    }
                    KeyPair keyPair = keyPairSet.getKeyPair(hostnames);
                    return visit(keyPair);
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for KeyPairSet: " + keyPairSet, e);
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
