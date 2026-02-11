/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <p>Runtime context provided to {@link ServerTlsCredentialSupplier} instances when
 * TLS credentials are requested.</p>
 *
 * <p>This context provides access to runtime information and resources that may be
 * needed when retrieving or generating TLS credentials. The context is implemented
 * by the Kroxylicious runtime and passed to the supplier's
 * {@link ServerTlsCredentialSupplier#tlsCredentials(ServerTlsCredentialSupplierContext)} method.</p>
 *
 * <p>The context exposes information about the client TLS connection and default
 * credentials configured for the target cluster. This information can be used to make
 * credential selection decisions based on client certificates, SNI, or other TLS handshake data.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * public class MyCredentialSupplier implements ServerTlsCredentialSupplier {
 *     @Override
 *     public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
 *         // Check if client presented a certificate
 *         Optional<ClientTlsContext> clientContext = context.clientTlsContext();
 *         if (clientContext.isPresent() && clientContext.get().clientCertificate().isPresent()) {
 *             // Use client cert info to select appropriate credentials
 *             return selectCredentialsBasedOnClient(clientContext.get());
 *         }
 *         // Fall back to shared default credentials
 *         try {
 *             byte[] certBytes = Files.readAllBytes(defaultCertPath);
 *             byte[] keyBytes = Files.readAllBytes(defaultKeyPath);
 *             return context.tlsCredentials(certBytes, keyBytes);
 *         } catch (IOException e) {
 *             return CompletableFuture.failedFuture(e);
 *         }
 *     }
 * }
 * }</pre>
 */
public interface ServerTlsCredentialSupplierContext {

    /**
     * <p>Returns TLS information about the client-to-proxy connection, if available.</p>
     *
     * <p>This provides access to the client's TLS certificate (if client authentication
     * was performed) and the proxy's server certificate that was presented to the client.
     * This information can be used to make credential selection decisions based on
     * client identity or other TLS handshake data.</p>
     *
     * @return Optional containing the client TLS context, or empty if TLS is not in use
     *         or if the handshake has not yet completed
     */
    @NonNull
    Optional<ClientTlsContext> clientTlsContext();

    /**
     * <p>Creates a TlsCredentials instance from PEM-encoded certificate and private key data.</p>
     *
     * <p>This factory method performs certificate validation before creating the TlsCredentials
     * instance. The validation ensures that:</p>
     * <ul>
     *   <li>The certificate chain is structurally valid</li>
     *   <li>The private key matches the certificate's public key</li>
     *   <li>The certificate dates are valid (not expired)</li>
     * </ul>
     *
     * <p>Additional validation will be performed by the Netty TLS layer when the credentials
     * are used to establish TLS connections, including:</p>
     * <ul>
     *   <li>Chain of trust verification</li>
     *   <li>Hostname verification (if configured)</li>
     *   <li>Protocol and cipher suite negotiation</li>
     * </ul>
     *
     * <p>The returned CompletionStage completes exceptionally if:</p>
     * <ul>
     *   <li>The certificate or key data is malformed or cannot be parsed</li>
     *   <li>The private key does not match the certificate</li>
     *   <li>Certificate validation fails</li>
     * </ul>
     *
     * @param certificateChainPem PEM-encoded certificate chain bytes (certificate and any intermediate certificates)
     * @param privateKeyPem PEM-encoded private key bytes (PKCS#8 or traditional format)
     * @return CompletionStage that completes with validated TlsCredentials or fails with validation errors
     */
    @NonNull
    default CompletionStage<TlsCredentials> tlsCredentials(@NonNull byte[] certificateChainPem, @NonNull byte[] privateKeyPem) {
        return tlsCredentials(certificateChainPem, privateKeyPem, null);
    }

    /**
     * <p>Creates a TlsCredentials instance from PEM-encoded certificate and private key data,
     * with an optional password for encrypted private keys.</p>
     *
     * <p>This method supports encrypted PKCS#8 private keys (EncryptedPrivateKeyInfo). When the
     * private key is encrypted, the password parameter must be provided to decrypt it. For
     * unencrypted private keys, the password should be {@code null}.</p>
     *
     * <p>This factory method performs the same validation as {@link #tlsCredentials(byte[], byte[])}.</p>
     *
     * @param certificateChainPem PEM-encoded certificate chain bytes (certificate and any intermediate certificates)
     * @param privateKeyPem PEM-encoded private key bytes (PKCS#8 or traditional format, may be encrypted)
     * @param password password for decrypting the private key, or {@code null} if the key is unencrypted
     * @return CompletionStage that completes with validated TlsCredentials or fails with validation errors
     */
    @NonNull
    CompletionStage<TlsCredentials> tlsCredentials(@NonNull byte[] certificateChainPem, @NonNull byte[] privateKeyPem,
                                                   @edu.umd.cs.findbugs.annotations.Nullable char[] password);
}
