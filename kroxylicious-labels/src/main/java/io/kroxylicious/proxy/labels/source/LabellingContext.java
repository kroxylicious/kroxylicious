/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels.source;

import java.util.Optional;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.tls.ClientTlsContext;

public interface LabellingContext {
    /**
     * @return The SASL context for the client connection, or empty if the client
     * has not successfully authenticated using SASL.
     */
    Optional<ClientSaslContext> clientSaslContext();

    /**
     * @return The TLS context for the client connection, or empty if the client connection is not TLS.
     */
    Optional<ClientTlsContext> clientTlsContext();
}
