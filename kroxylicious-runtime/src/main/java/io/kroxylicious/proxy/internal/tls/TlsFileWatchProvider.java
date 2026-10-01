/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.nio.file.Path;
import java.util.Objects;

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.KeyProviderVisitor;
import io.kroxylicious.proxy.config.tls.KeyStore;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustProviderVisitor;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.util.FileWatcher;
import io.kroxylicious.proxy.internal.util.FileWatcher.NotificationHandler;

/**
 * Adds certificates and keys from a TLS configuration to an existing FileWatcher.
 *
 * @see io.kroxylicious.proxy.internal.util.FileWatcher
 */
public class TlsFileWatchProvider {
    private final Tls delegate;

    /**
     * Create a file watcher for watching certificates
     * @param tls The Tls configuration contaiing the certificate file paths
     */
    public TlsFileWatchProvider(final Tls tls) {
        this.delegate = tls;
    }

    /**
     * Apply the supplied TLS {@link #TlsFileWatchProvider(Tls)} configuration to a file watcher
     *
     * @param fileWatcher watcher to use
     * @param listener the lambda to invoke when a file change is detected
     * @return the configured watcher
     */
    public FileWatcher apply(final FileWatcher fileWatcher, final NotificationHandler listener) {

        if (this.delegate.definesKey()) {
            this.delegate.key().accept(new KeyProviderVisitor<>() {

                @Override
                public FileWatcher visit(KeyPair keyPair) {
                    fileWatcher.register(Path.of(keyPair.privateKeyFile()), listener);
                    fileWatcher.register(Path.of(keyPair.certificateFile()), listener);
                    return fileWatcher;
                }

                @Override
                public FileWatcher visit(KeyStore keyStore) {
                    fileWatcher.register(Path.of(keyStore.storeFile()), listener);
                    return fileWatcher;
                }

            });
        }

        if (Objects.nonNull(this.delegate.trust())) {
            this.delegate.trust().accept(new TrustProviderVisitor<>() {

                @Override
                public FileWatcher visit(TrustStore trustStore) {
                    return fileWatcher.register(Path.of(trustStore.storeFile()), listener);
                }

                @Override
                public FileWatcher visit(InsecureTls insecureTls) {
                    return fileWatcher; // no files to watch for insecure mode
                }

                @Override
                public FileWatcher visit(PlatformTrustProvider platformTrustProviderTls) {
                    return fileWatcher; // no files to watch
                }

            });
        }

        return fileWatcher;
    }
}
