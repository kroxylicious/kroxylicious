/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import static org.assertj.core.api.Assertions.assertThat;

class SaslSubjectBuilderImplTest {

    @Test
    void shouldBuildSubjectWithUserNamedAfterAuthorizationId() {
        // Given
        var builder = new SaslSubjectBuilderImpl();
        var context = new SaslSubjectBuilder.Context() {
            @Override
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            public ClientSaslContext clientSaslContext() {
                return new ClientSaslContext() {
                    @Override
                    public String mechanismName() {
                        return "PLAIN";
                    }

                    @Override
                    public String authorizationId() {
                        return "alice";
                    }
                };
            }
        };

        // When
        var subject = builder.buildSaslSubject(context);

        // Then
        assertThat(subject).isCompleted();
        assertThat(subject.toCompletableFuture().join())
                .isEqualTo(new Subject(Set.of(new User("alice"))));
    }
}
