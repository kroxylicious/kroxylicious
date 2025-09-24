/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization.subject;

import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.authorizer.service.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

/**
 * Builds a {@link Subject} based on information
 * proven by, known about, or provided by it.
 */
public interface ClientSubjectBuilder {

    CompletionStage<Subject> buildSubject(Context context);

    interface Context {
        SocketAddress clientAddress();
        String clientId();
        Optional<ClientTlsContext> clientTlsContext();
        Optional<String> saslAuthorizedId();
    }
}
