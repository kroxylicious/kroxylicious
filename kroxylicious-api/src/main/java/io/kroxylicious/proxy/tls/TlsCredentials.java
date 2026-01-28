/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

/**
 * <p>Represents TLS credentials (private key and certificate chain) that can be used
 * for server-side TLS connections.</p>
 *
 * <p>This interface is intentionally empty as it serves as a marker interface.
 * The runtime implementation will contain the actual credential data and provide
 * necessary integration with the underlying TLS/SSL infrastructure.</p>
 *
 * <p>Plugin developers should not implement this interface directly. Instead,
 * implementations are provided by the Kroxylicious runtime when TLS credentials
 * are successfully loaded from the configured source.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Received from ServerTlsCredentialSupplier
 * CompletionStage<TlsCredentials> credentialsStage = supplier.tlsCredentials(context);
 * credentialsStage.thenAccept(credentials -> {
 *     // Runtime uses these credentials to configure TLS
 * });
 * }</pre>
 */
public interface TlsCredentials {
    // Intentionally empty - runtime implementation provides actual credential data
}
