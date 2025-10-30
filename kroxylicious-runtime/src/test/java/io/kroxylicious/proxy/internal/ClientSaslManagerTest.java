/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

class ClientSaslManagerTest {

    @Test
    void initialState() {
        // Given
        ClientSaslManager impl = new ClientSaslManager(null, null, Subject.anonymous());
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionInitialToAuthorized() {
        // Given
        ClientSaslManager impl = new ClientSaslManager(null, null, Subject.anonymous());
        // When
        impl.clientSaslAuthenticationSuccess("FOO", "bob", new Subject(new User("bob")));
        // Then
        assertThat(impl.clientSaslContext()).hasValueSatisfying(csc -> {
            assertThat(csc.mechanismName()).isEqualTo("FOO");
            assertThat(csc.authorizationId()).isEqualTo("bob");
        });
    }

    @Test
    void transitionInitialToFailed() {
        // Given
        ClientSaslManager impl = new ClientSaslManager(null, null, Subject.anonymous());
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionAuthorizedToAuthorized() {
        // Given
        ClientSaslManager impl = new ClientSaslManager(null, null, Subject.anonymous());
        impl.clientSaslAuthenticationSuccess("FOO", "bob", new Subject(new User("bob")));
        // When
        impl.clientSaslAuthenticationSuccess("BAR", "sue", new Subject(new User("sue")));
        // Then
        assertThat(impl.clientSaslContext()).hasValueSatisfying(csc -> {
            assertThat(csc.mechanismName()).isEqualTo("BAR");
            assertThat(csc.authorizationId()).isEqualTo("sue");
        });
    }

    @Test
    void transitionAuthorizedToFailed() {
        // Given
        ClientSaslManager impl = new ClientSaslManager(null, null, Subject.anonymous());
        impl.clientSaslAuthenticationSuccess("FOO", "bob", new Subject(new User("bob")));
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionFailedToAuthorized() {
        // Given
        ClientSaslManager impl = new ClientSaslManager(null, null, Subject.anonymous());
        impl.clientSaslAuthenticationFailure();

        // When
        impl.clientSaslAuthenticationSuccess("FOO", "bob", new Subject(new User("bob")));
        // Then
        assertThat(impl.clientSaslContext()).hasValueSatisfying(csc -> {
            assertThat(csc.mechanismName()).isEqualTo("FOO");
            assertThat(csc.authorizationId()).isEqualTo("bob");
        });
    }

}
