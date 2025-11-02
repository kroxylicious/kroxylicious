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
 * <p>Builds a {@link Subject} based on information available at the transport layer,
 * before any requests have been received from the client.</p>
 *
 * <p>A {@code TransportSubjectBuilder} instance is constructed by a {@link TransportSubjectBuilderService},
 * which in turn is specified on a virtual cluster.</p>
 *
 * <p>See {@link SaslSubjectBuilder} for a similar interface use for building a {@code Subject} based on SASL authentication.</p>
 */
public interface TransportSubjectBuilder {

    CompletionStage<Subject> buildTransportSubject(Context context);

    interface Context {
        Optional<ClientTlsContext> clientTlsContext();
    }
}
