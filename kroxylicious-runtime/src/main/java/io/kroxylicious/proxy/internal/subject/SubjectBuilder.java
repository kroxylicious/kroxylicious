/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

/**
 * Builds a {@link Subject} based on information
 * proven by, known about, or provided by it.
 */
public interface SubjectBuilder {

    CompletionStage<Subject> buildSubject(Context context);

    interface Context {
        Optional<ClientTlsContext> clientTlsContext();

        Optional<ClientSaslContext> clientSaslContext();
    }
}
