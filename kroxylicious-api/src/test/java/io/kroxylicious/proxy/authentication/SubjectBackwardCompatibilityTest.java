/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kroxylicious.identity.Identity;

/**
 * Tests proving backward compatibility of old Subject/Principal types
 * with the new identity API types.
 */
@SuppressWarnings("deprecation")
class SubjectBackwardCompatibilityTest {

    @Test
    void oldSubjectIsAnIdentity() {
        // Given
        Subject subject = new Subject(new User("alice"));

        // When
        Identity identity = subject;

        // Then
        Assertions.assertThat(identity.isAnonymous()).isFalse();
        Assertions.assertThat(identity.principals()).hasSize(1);
    }

    @Test
    void oldPrincipalIsANewPrincipal() {
        // Given
        User user = new User("alice");

        // When
        io.kroxylicious.identity.Principal newPrincipal = user;

        // Then
        Assertions.assertThat(newPrincipal.name()).isEqualTo("alice");
    }

    @Test
    void oldSubjectPrincipalsAccessibleViaIdentityInterface() {
        // Given
        User user = new User("alice");
        FakeMultiplePrincipal extra = new FakeMultiplePrincipal("role1");
        Subject subject = new Subject(user, extra);

        // When
        Identity identity = subject;

        // Then
        Assertions.assertThat(identity.principals()).isEqualTo(Set.of(user, extra));
    }

    @Test
    void identityMethodsWorkWithOldPrincipalTypes() {
        // Given
        User user = new User("alice");
        FakeUniquePrincipal unique = new FakeUniquePrincipal("special");
        FakeMultiplePrincipal foo = new FakeMultiplePrincipal("foo");
        FakeMultiplePrincipal bar = new FakeMultiplePrincipal("bar");
        Subject subject = new Subject(user, unique, foo, bar);
        Identity identity = subject;

        // Then
        Assertions.assertThat(identity.uniquePrincipalOfType(User.class)).hasValue(user);
        Assertions.assertThat(identity.uniquePrincipalOfType(FakeUniquePrincipal.class)).hasValue(unique);
        Assertions.assertThat(identity.allPrincipalsOfType(FakeMultiplePrincipal.class)).containsExactlyInAnyOrder(foo, bar);
    }

    @Test
    void anonymousOldSubjectIsAnonymousIdentity() {
        // Given
        Subject subject = Subject.anonymous();

        // When
        Identity identity = subject;

        // Then
        Assertions.assertThat(identity.isAnonymous()).isTrue();
        Assertions.assertThat(identity.principals()).isEmpty();
    }

    @Test
    void oldUniqueAnnotationEnforcedInConstructor() {
        // Given
        User user = new User("alice");
        FakeOldUniquePrincipal old1 = new FakeOldUniquePrincipal("a");
        FakeOldUniquePrincipal old2 = new FakeOldUniquePrincipal("b");

        // When / Then
        Assertions.assertThatThrownBy(() -> new Subject(user, old1, old2))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void oldUniqueAnnotationAcceptedByUniquePrincipalOfType() {
        // Given
        User user = new User("alice");
        FakeOldUniquePrincipal oldUnique = new FakeOldUniquePrincipal("special");
        Subject subject = new Subject(user, oldUnique);

        // When / Then
        Assertions.assertThat(subject.uniquePrincipalOfType(FakeOldUniquePrincipal.class)).hasValue(oldUnique);
    }

    @Test
    void oldSubjectCanBeMixedWithNewPrincipalTypes() {
        // Given
        User user = new User("alice");
        FakeUniquePrincipal newStylePrincipal = new FakeUniquePrincipal("modern");
        Subject subject = new Subject(user, newStylePrincipal);

        // When
        Identity identity = subject;

        // Then
        Assertions.assertThat(identity.uniquePrincipalOfType(FakeUniquePrincipal.class)).hasValue(newStylePrincipal);
        Assertions.assertThat(identity.uniquePrincipalOfType(User.class)).hasValue(user);
    }
}
