/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.ClientSaslContext;

import static org.assertj.core.api.Assertions.assertThat;

class ClientSaslManagerTest {

    @Test
    void initialState() {
        // Given
        ClientSaslManager impl = new ClientSaslManager();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionInitialToAuthorized() {
        // Given
        ClientSaslManager impl = new ClientSaslManager();
        // When
        impl.clientSaslAuthenticationSuccess("FOO", "bob");
        // Then
        AbstractObjectAssert<?, ClientSaslContext> csc = assertThat(impl.clientSaslContext()).get();
        csc.extracting(ClientSaslContext::mechanismName).isEqualTo("FOO");
        csc.extracting(ClientSaslContext::authorizationId).isEqualTo("bob");
    }

    @Test
    void transitionInitialToFailed() {
        // Given
        ClientSaslManager impl = new ClientSaslManager();
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionAuthorizedToAuthorized() {
        // Given
        ClientSaslManager impl = new ClientSaslManager();
        impl.clientSaslAuthenticationSuccess("FOO", "bob");
        // When
        impl.clientSaslAuthenticationSuccess("BAR", "sue");
        // Then
        AbstractObjectAssert<?, ClientSaslContext> csc = assertThat(impl.clientSaslContext()).get();
        csc.extracting(ClientSaslContext::mechanismName).isEqualTo("BAR");
        csc.extracting(ClientSaslContext::authorizationId).isEqualTo("sue");
    }

    @Test
    void transitionAuthorizedToFailed() {
        // Given
        ClientSaslManager impl = new ClientSaslManager();
        impl.clientSaslAuthenticationSuccess("FOO", "bob");
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionFailedToAuthorized() {
        // Given
        ClientSaslManager impl = new ClientSaslManager();
        impl.clientSaslAuthenticationFailure();

        // When
        impl.clientSaslAuthenticationSuccess("FOO", "bob");
        // Then
        AbstractObjectAssert<?, ClientSaslContext> csc2 = assertThat(impl.clientSaslContext()).get();
        csc2.extracting(ClientSaslContext::mechanismName).isEqualTo("FOO");
        csc2.extracting(ClientSaslContext::authorizationId).isEqualTo("bob");
    }

}