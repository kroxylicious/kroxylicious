/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <p>Runtime context provided to {@link ServerTlsCredentialSupplier} instances when
 * TLS credentials are requested.</p>
 *
 * <p>This context provides access to runtime information and a factory method for
 * creating validated {@link TlsCredentials} instances from JDK cryptographic objects.
 * The context is implemented by the Kroxylicious runtime and passed to the supplier's
 * {@link ServerTlsCredentialSupplier#tlsCredentials(ServerTlsCredentialSupplierContext)} method.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * public class MyCredentialSupplier implements ServerTlsCredentialSupplier {
 *     private final PrivateKey defaultKey;
 *     private final Certificate[] defaultChain;
 *
 *     @Override
 *     public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
 *         TlsCredentials creds = context.tlsCredentials(defaultKey, defaultChain);
 *         return CompletableFuture.completedFuture(creds);
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
     * <p>Creates a {@link TlsCredentials} instance from the given private key and certificate chain.</p>
     *
     * <p>This factory method validates the provided credentials before creating the
     * {@link TlsCredentials} instance. The validation ensures that:</p>
     * <ul>
     *   <li>The certificate chain is structurally valid</li>
     *   <li>The private key matches the leaf certificate's public key</li>
     * </ul>
     *
     * <p>The plugin is responsible for loading and parsing the credentials from whatever
     * source and format it uses (PEM files, PKCS12 keystores, HSMs, etc.).</p>
     *
     * @param key The private key corresponding to the leaf certificate.
     * @param certificateChain The certificate chain, starting with the leaf certificate
     *        and including any intermediate certificates up to (but not including) the root CA.
     * @return Validated TlsCredentials instance
     * @throws IllegalArgumentException if the key does not match the certificate or the chain is invalid
     * @see ServerTlsCredentialSupplierFactoryContext#tlsCredentials(PrivateKey, Certificate[])
     */
    @NonNull
    TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain);
}
