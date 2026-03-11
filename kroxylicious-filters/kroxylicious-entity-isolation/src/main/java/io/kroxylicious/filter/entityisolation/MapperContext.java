/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Mapper context.
 *
 * @param authenticatedSubject authentication subject
 * @param clientTlsContext client TLS context - will be null if the channel is not TLS.
 * @param clientSaslContext client SASL context - will be null if channel has not negotiated SASL.
 */
public record MapperContext(Subject authenticatedSubject, @Nullable ClientTlsContext clientTlsContext, @Nullable ClientSaslContext clientSaslContext) {
    public MapperContext {
        Objects.requireNonNull(authenticatedSubject);
    }
}
