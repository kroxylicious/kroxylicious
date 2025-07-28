/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.proxy.authentication.ClientSaslContext;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;

@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
public class ClientSaslContextImpl {

    private record Authorized(
                              String authorizationId,
                              String mechanismName)
            implements ClientSaslContext {}

    private Authorized clientAuthorization;

    public ClientSaslContextImpl() {
        this.clientAuthorization = null;
    }

    void clientSaslAuthenticationSuccess(String mechanism,
                                         String clientAuthorizationId) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(clientAuthorizationId, "clientAuthorizationId");
        this.clientAuthorization = new Authorized(clientAuthorizationId, mechanism);
    }

    void clientSaslAuthenticationFailure() {
        this.clientAuthorization = null;
    }

    public Optional<ClientSaslContext> clientSaslContext() {
        return Optional.ofNullable(clientAuthorization);
    }
}
