/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.io.InputStream;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.tls.KeyProvider;
import io.kroxylicious.proxy.config.tls.NettyKeyProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierContext;
import io.kroxylicious.proxy.tls.TlsCredentials;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Runtime implementation of ServerTlsCredentialSupplierContext.
 */
public class ServerTlsCredentialSupplierContextImpl implements ServerTlsCredentialSupplierContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerTlsCredentialSupplierContextImpl.class);

    private final Optional<ClientTlsContext> clientTlsContext;
    private final Optional<Tls> targetClusterTls;

    public ServerTlsCredentialSupplierContextImpl(@Nullable ClientTlsContext clientTlsContext,
                                                  @NonNull Optional<Tls> targetClusterTls) {
        this.clientTlsContext = Optional.ofNullable(clientTlsContext);
        this.targetClusterTls = Objects.requireNonNull(targetClusterTls);
    }

    @NonNull
    @Override
    public Optional<ClientTlsContext> clientTlsContext() {
        return clientTlsContext;
    }

    @NonNull
    @Override
    public CompletionStage<TlsCredentials> defaultTlsCredentials() {
        if (targetClusterTls.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No default TLS credentials configured for target cluster"));
        }

        Tls tls = targetClusterTls.get();
        KeyProvider keyProvider = tls.key();
        if (keyProvider == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No key provider configured in target cluster TLS configuration"));
        }

        try {
            // Build a temporary SslContext to extract the private key and certificate chain
            SslContextBuilder builder = new NettyKeyProvider(keyProvider).forClient();
            SslContext sslContext = builder.build();

            // Extract credentials from the SslContext
            // Note: This is a simplified approach. In production, we'd need to properly extract
            // the key material from the SslContext or directly from the KeyProvider
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException(
                            "Default credentials extraction not yet implemented. " +
                                    "Please use tlsCredentials(InputStream, InputStream) method to create credentials."));
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Failed to load default TLS credentials from target cluster configuration", e));
        }
    }

    @NonNull
    @Override
    public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
        Objects.requireNonNull(certificateChainPem, "certificateChainPem must not be null");
        Objects.requireNonNull(privateKeyPem, "privateKeyPem must not be null");

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Parse the private key and certificate chain from PEM data
                PrivateKey privateKey = TlsUtil.parsePrivateKey(privateKeyPem);
                X509Certificate[] certChain = TlsUtil.parseCertificateChain(certificateChainPem);

                // Perform comprehensive certificate validation
                // This validates:
                // - Certificate chain structure and order
                // - Private key corresponds to the certificate
                // - Certificate validity dates
                // - Chain integrity (signatures)
                // - No root CA included (per API contract)
                TlsUtil.validateCertificateChain(privateKey, certChain);

                // Additional validation through Netty to ensure the credentials work with the TLS layer
                try {
                    SslContextBuilder builder = SslContextBuilder.forClient()
                            .keyManager(privateKey, certChain);
                    builder.build();
                }
                catch (Exception e) {
                    throw new IllegalStateException(
                            "Credentials validation failed at Netty TLS layer: " + e.getMessage() +
                                    ". The certificate chain or private key may be incompatible with the TLS implementation.",
                            e);
                }

                return new TlsCredentialsImpl(privateKey, certChain);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                // Re-throw validation exceptions with original message
                LOGGER.error("TLS credentials validation failed: {}", e.getMessage());
                throw e;
            }
            catch (Exception e) {
                LOGGER.error("Failed to create TLS credentials from PEM data", e);
                throw new IllegalStateException("Failed to parse or validate TLS credentials: " + e.getMessage(), e);
            }
        });
    }
}
