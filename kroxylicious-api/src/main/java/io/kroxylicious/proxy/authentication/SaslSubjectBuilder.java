/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.authentication;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.tls.ClientTlsContext;

/**
 * <p>Builds a {@link Subject} based on information available from a successful SASL authentication.</p>
 *
 * <p>See {@link TransportSubjectBuilder} for a similar interface use for building a
 * {@code Subject} based on transport-layer information.</p>
 */
public interface SaslSubjectBuilder {

    /**
     * Returns an asynchronous result which completes with the {@code Subject} built
     * from the
     * @param context
     * @return
     */
    CompletionStage<Subject> buildSaslSubject(SaslSubjectBuilder.Context context);

    interface Context {
        Optional<ClientTlsContext> clientTlsContext();

        ClientSaslContext clientSaslContext();
    }
}
