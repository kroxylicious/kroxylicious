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
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;

@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
public class ClientSaslContextImpl implements ClientSaslContext {

    private @Nullable String clientAuthorizationId;
    private @Nullable String mechanism;

    public ClientSaslContextImpl() {
        this.clientAuthorizationId = null;
    }

    void clientSaslAuthenticationSuccess(String mechanism,
                                         String clientAuthorizationId) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(clientAuthorizationId, "clientAuthorizationId");
        this.clientAuthorizationId = clientAuthorizationId;
        this.mechanism = mechanism;
    }

    void clientSaslAuthenticationFailure() {
        this.clientAuthorizationId = null;
        this.mechanism = null;
    }

    public Optional<ClientSaslContext> clientSaslContext() {
        if (clientAuthorizationId != null) {
            return Optional.of(this);
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public String mechanismName() {
        // A Filter implementation should never get an NPE as a result of calling this method
        // because FilterContext.clientSaslContext() would return an empty
        // optional if there was no current SASL authentication information.
        return Objects.requireNonNull(this.mechanism);
    }

    @Override
    public String authorizationId() {
        // A Filter implementation should never get an NPE as a result of calling this method
        // because FilterContext.clientSaslContext() would return an empty
        // optional if there was no current SASL authentication information.
        return Objects.requireNonNull(this.clientAuthorizationId);
    }

}
