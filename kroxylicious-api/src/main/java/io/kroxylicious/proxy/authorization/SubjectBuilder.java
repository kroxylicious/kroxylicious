/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Builds a {@link Subject} based on information
 * proven by, known about, or provided by it.
 */
public interface SubjectBuilder {

    interface Context {
        InetSocketAddress clientIp();
        String clientId();
        Optional<X509Certificate> x509Certificate();
        Optional<String> saslAuthorizedId();
    }

    CompletionStage<Subject> buildSubject(Context context);
}
