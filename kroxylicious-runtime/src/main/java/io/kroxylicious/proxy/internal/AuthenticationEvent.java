/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.Objects;

/**
 * Sent as a Netty "User Event" by the {@link KafkaAuthnHandler}
 * to handlers interested in the authenticated user.
 */
public class AuthenticationEvent {
    private final String authorizationId;
    private final Map<String, Object> negotiatedProperties;

    public AuthenticationEvent(String authorizationId, Map<String, Object> negotiatedProperties) {
        this.authorizationId = authorizationId;
        this.negotiatedProperties = Map.copyOf(negotiatedProperties);
    }

    /**
     * The id that the user authorized with
     */
    public String authorizationId() {
        return authorizationId;
    }

    /**
     * Extra authorization state produced by
     * the authentication process.
     */
    public Map<String, Object> negotiatedProperties() {
        return negotiatedProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthenticationEvent that = (AuthenticationEvent) o;
        return Objects.equals(authorizationId(), that.authorizationId()) && Objects.equals(negotiatedProperties(), that.negotiatedProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizationId(), negotiatedProperties());
    }
}
