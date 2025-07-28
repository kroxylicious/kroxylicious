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

class ClientSaslContextImplTest {

    @Test
    void initialState() {
        // Given
        ClientSaslContextImpl impl = new ClientSaslContextImpl();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionInitialToAuthorized() {
        // Given
        ClientSaslContextImpl impl = new ClientSaslContextImpl();
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
        ClientSaslContextImpl impl = new ClientSaslContextImpl();
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionAuthorizedToAuthorized() {
        // Given
        ClientSaslContextImpl impl = new ClientSaslContextImpl();
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
        ClientSaslContextImpl impl = new ClientSaslContextImpl();
        impl.clientSaslAuthenticationSuccess("FOO", "bob");
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionFailedToAuthorized() {
        // Given
        ClientSaslContextImpl impl = new ClientSaslContextImpl();
        impl.clientSaslAuthenticationFailure();

        // When
        impl.clientSaslAuthenticationSuccess("FOO", "bob");
        // Then
        AbstractObjectAssert<?, ClientSaslContext> csc2 = assertThat(impl.clientSaslContext()).get();
        csc2.extracting(ClientSaslContext::mechanismName).isEqualTo("FOO");
        csc2.extracting(ClientSaslContext::authorizationId).isEqualTo("bob");
    }

}