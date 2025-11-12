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
 * <p>A {@code SaslSubjectBuilder} instance is constructed by a {@link SaslSubjectBuilderService}.</p>
 *
 * <p>A SASL-authenticating {@link io.kroxylicious.proxy.filter.Filter Filter}
 * <em>may</em> use a {@code SaslSubjectBuilder} in order to construct the
 * {@link Subject} with which it calls
 * {@link io.kroxylicious.proxy.filter.FilterContext#clientSaslAuthenticationSuccess(String, Subject)
 * FilterContext.clientSaslAuthenticationSuccess(String, Subject)}.
 * As such, {@code SaslSubjectBuilder} is an opt-in way of decoupling the building of Subjects
 * from the mechanism of SASL authentication.
 * SASL-authenticating filters are not obliged to use this abstraction.</p>
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
