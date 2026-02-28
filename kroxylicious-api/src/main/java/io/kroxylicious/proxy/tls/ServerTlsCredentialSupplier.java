/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.util.concurrent.CompletionStage;

/**
 * <p>Supplies TLS credentials for proxy-to-server (upstream) TLS connections.</p>
 *
 * <p>Instances of this interface are created by {@link ServerTlsCredentialSupplierFactory}
 * and are responsible for providing TLS credentials (private keys and certificate chains)
 * that the proxy uses when connecting to the target Kafka cluster.</p>
 *
 * <p>The supplier supports asynchronous credential retrieval, allowing implementations
 * to load credentials from remote sources, perform cryptographic operations, or
 * interact with external services without blocking the proxy runtime.</p>
 *
 * <h2>Thread Safety</h2>
 * <p>Implementations must be thread-safe as the {@link #tlsCredentials(ServerTlsCredentialSupplierContext)}
 * method may be called concurrently from multiple threads.</p>
 *
 * <h2>Non-Blocking Requirement</h2>
 * <p>Implementations must not block the calling thread or perform heavy I/O operations synchronously.
 * Long-running work such as network calls, file I/O, or key generation should be performed
 * asynchronously, returning a {@link CompletionStage} that completes when the work is done.</p>
 *
 * <h2>Error Handling</h2>
 * <p>If credential retrieval fails, implementations should return a {@link CompletionStage}
 * that completes exceptionally. The runtime will handle the exception appropriately,
 * typically by rejecting the connection attempt.</p>
 *
 * <h2>Usage Example: File-Based Credential Loading</h2>
 * <pre>{@code
 * public class FileBasedCredentialSupplier implements ServerTlsCredentialSupplier {
 *     private final PrivateKey key;
 *     private final Certificate[] chain;
 *
 *     public FileBasedCredentialSupplier(PrivateKey key, Certificate[] chain) {
 *         this.key = key;
 *         this.chain = chain;
 *     }
 *
 *     @Override
 *     public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
 *         // Plugin has already parsed the key and certificate chain (from PEM, PKCS12, etc.)
 *         // Use context factory method to create validated TlsCredentials
 *         TlsCredentials creds = context.tlsCredentials(key, chain);
 *         return CompletableFuture.completedFuture(creds);
 *     }
 * }
 * }</pre>
 *
 * <h2>Usage Example: Client-Specific Credentials</h2>
 * <pre>{@code
 * public class ClientSpecificSupplier implements ServerTlsCredentialSupplier {
 *     private final Map<String, PrivateKey> clientKeys;
 *     private final Map<String, Certificate[]> clientChains;
 *     private final PrivateKey defaultKey;
 *     private final Certificate[] defaultChain;
 *
 *     @Override
 *     public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
 *         Optional<ClientTlsContext> clientContext = context.clientTlsContext();
 *
 *         if (clientContext.isPresent() && clientContext.get().clientCertificate().isPresent()) {
 *             String clientId = clientContext.get().clientCertificate().get()
 *                 .getSubjectX500Principal().getName();
 *
 *             PrivateKey key = clientKeys.get(clientId);
 *             Certificate[] chain = clientChains.get(clientId);
 *
 *             if (key != null && chain != null) {
 *                 TlsCredentials creds = context.tlsCredentials(key, chain);
 *                 return CompletableFuture.completedFuture(creds);
 *             }
 *         }
 *
 *         // Fall back to shared default credentials
 *         TlsCredentials creds = context.tlsCredentials(defaultKey, defaultChain);
 *         return CompletableFuture.completedFuture(creds);
 *     }
 * }
 * }</pre>
 *
 * @see ServerTlsCredentialSupplierFactory
 * @see TlsCredentials
 */
public interface ServerTlsCredentialSupplier {

    /**
     * <p>Asynchronously retrieves TLS credentials for the proxy to use when connecting
     * to the target Kafka cluster.</p>
     *
     * <p>This method may be called multiple times and should return credentials
     * appropriate for the current request context. Implementations may cache
     * credentials, retrieve them from external sources, or generate them on-demand.</p>
     *
     * @param context The runtime context for this credential request
     * @return A {@link CompletionStage} that completes with the TLS credentials
     */
    CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context);
}
