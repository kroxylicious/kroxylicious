/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.identity.Identity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that {@link AuthorizeResult} accepts both the general-purpose
 * {@link io.kroxylicious.identity.Subject} and the deprecated proxy
 * {@link io.kroxylicious.proxy.authentication.Subject} via the {@link Identity} bridge.
 */
class AuthorizerBridgeTest {

    record Role(String name) implements io.kroxylicious.identity.Principal {}

    @Test
    void shouldAcceptIdentitySubject() {
        // Given
        Identity subject = new io.kroxylicious.identity.Subject(java.util.Set.of(new Role("admin")));

        // When
        var result = new AuthorizeResult(subject, List.of(), List.of());

        // Then
        assertThat(result.subject()).isSameAs(subject);
    }

    @Test
    void shouldAcceptDeprecatedProxySubject() {
        // Given
        Identity subject = new io.kroxylicious.proxy.authentication.Subject(new io.kroxylicious.proxy.authentication.User("alice"));

        // When
        var result = new AuthorizeResult(subject, List.of(), List.of());

        // Then
        assertThat(result.subject()).isSameAs(subject);
    }
}
