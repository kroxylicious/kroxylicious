/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContextBuilder;

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

    public ServerTlsCredentialSupplierContextImpl(@Nullable ClientTlsContext clientTlsContext) {
        this.clientTlsContext = Optional.ofNullable(clientTlsContext);
    }

    @NonNull
    @Override
    public Optional<ClientTlsContext> clientTlsContext() {
        return clientTlsContext;
    }

    @NonNull
    @Override
    public CompletionStage<TlsCredentials> tlsCredentials(@NonNull byte[] certificateChainPem, @NonNull byte[] privateKeyPem,
                                                          @edu.umd.cs.findbugs.annotations.Nullable char[] password) {
        Objects.requireNonNull(certificateChainPem, "certificateChainPem must not be null");
        Objects.requireNonNull(privateKeyPem, "privateKeyPem must not be null");

        try {
            // Parse the private key and certificate chain from PEM data
            PrivateKey privateKey = TlsUtil.parsePrivateKey(privateKeyPem, password);
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

            return CompletableFuture.completedFuture(new TlsCredentialsImpl(privateKey, certChain));
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            // Re-throw validation exceptions with original message
            LOGGER.error("TLS credentials validation failed: {}", e.getMessage());
            return CompletableFuture.failedFuture(e);
        }
        catch (Exception e) {
            LOGGER.error("Failed to create TLS credentials from PEM data", e);
            return CompletableFuture.failedFuture(new IllegalStateException("Failed to parse or validate TLS credentials: " + e.getMessage(), e));
        }
    }
}
