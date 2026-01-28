/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.util.concurrent.CompletionStage;

/**
 * <p>Supplies TLS credentials for server-side TLS connections.</p>
 *
 * <p>Instances of this interface are created by {@link ServerTlsCredentialSupplierFactory}
 * and are responsible for providing TLS credentials (private keys and certificate chains)
 * that the proxy can use when accepting TLS connections from clients.</p>
 *
 * <p>The supplier supports asynchronous credential retrieval, allowing implementations
 * to load credentials from remote sources, perform cryptographic operations, or
 * interact with external services without blocking the proxy runtime.</p>
 *
 * <h2>Thread Safety</h2>
 * <p>Implementations must be thread-safe as the {@link #tlsCredentials(ServerTlsCredentialSupplierContext)}
 * method may be called concurrently from multiple threads.</p>
 *
 * <h2>Error Handling</h2>
 * <p>If credential retrieval fails, implementations should return a {@link CompletionStage}
 * that completes exceptionally. The runtime will handle the exception appropriately,
 * typically by rejecting the TLS connection attempt.</p>
 *
 * <h2>Usage Example: File-Based Credential Loading</h2>
 * <pre>{@code
 * public class FileBasedCredentialSupplier implements ServerTlsCredentialSupplier {
 *     private final Path keyPath;
 *     private final Path certPath;
 *
 *     public FileBasedCredentialSupplier(Path keyPath, Path certPath) {
 *         this.keyPath = keyPath;
 *         this.certPath = certPath;
 *     }
 *
 *     @Override
 *     public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
 *         try {
 *             // Load PEM-encoded certificate and private key from files
 *             InputStream certStream = Files.newInputStream(certPath);
 *             InputStream keyStream = Files.newInputStream(keyPath);
 *
 *             // Use context factory method to create validated TlsCredentials
 *             return context.tlsCredentials(certStream, keyStream);
 *         }
 *         catch (IOException e) {
 *             return CompletableFuture.failedFuture(e);
 *         }
 *     }
 * }
 * }</pre>
 *
 * <h2>Usage Example: Client-Specific Credentials</h2>
 * <pre>{@code
 * public class ClientSpecificSupplier implements ServerTlsCredentialSupplier {
 *     private final Map<String, Path> clientCertPaths;
 *     private final Map<String, Path> clientKeyPaths;
 *
 *     @Override
 *     public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
 *         // Access client TLS context to determine which credentials to use
 *         Optional<ClientTlsContext> clientContext = context.clientTlsContext();
 *
 *         if (clientContext.isPresent() && clientContext.get().clientCertificate().isPresent()) {
 *             // Extract client identity from certificate
 *             String clientId = clientContext.get().clientCertificate().get()
 *                 .getSubjectX500Principal().getName();
 *
 *             // Load client-specific credentials
 *             Path certPath = clientCertPaths.get(clientId);
 *             Path keyPath = clientKeyPaths.get(clientId);
 *
 *             if (certPath != null && keyPath != null) {
 *                 try {
 *                     return context.tlsCredentials(
 *                         Files.newInputStream(certPath),
 *                         Files.newInputStream(keyPath)
 *                     );
 *                 }
 *                 catch (IOException e) {
 *                     return CompletableFuture.failedFuture(e);
 *                 }
 *             }
 *         }
 *
 *         // Fall back to default credentials from configuration
 *         return context.defaultTlsCredentials();
 *     }
 * }
 * }</pre>
 *
 * @see ServerTlsCredentialSupplierFactory
 * @see TlsCredentials
 */
public interface ServerTlsCredentialSupplier {

    /**
     * <p>Asynchronously retrieves TLS credentials for the proxy to use when accepting
     * client connections.</p>
     *
     * <p>This method may be called multiple times and should return credentials
     * appropriate for the current request context. Implementations may cache
     * credentials, retrieve them from external sources, or generate them on-demand.</p>
     *
     * <p>The returned {@link CompletionStage} should complete with a {@link TlsCredentials}
     * instance containing the private key and certificate chain. If credential retrieval
     * fails, the stage should complete exceptionally.</p>
     *
     * @param context The runtime context for this credential request
     * @return A {@link CompletionStage} that completes with the TLS credentials
     */
    CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context);
}
