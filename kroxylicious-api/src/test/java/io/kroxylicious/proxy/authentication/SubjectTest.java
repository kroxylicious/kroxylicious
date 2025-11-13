/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class SubjectTest {

    User user1 = new User("name");
    User user2 = new User("name2");

    FakeUniquePrincipal unique = new FakeUniquePrincipal("name");
    FakeUniquePrincipal unique2 = new FakeUniquePrincipal("name2");
    FakeMultiplePrincipal foo = new FakeMultiplePrincipal("foo");
    FakeMultiplePrincipal bar = new FakeMultiplePrincipal("bar");

    @Test
    void userIsRequiredRightNow() { // but not eventually
        Assertions.assertThatThrownBy(() -> new Subject(unique))
                .hasMessage("A subject with non-empty principals must have exactly one io.kroxylicious.proxy.authentication.User principal.");
    }

    @Test
    void uniquenessIsEnforced() { // but not eventually
        Assertions.assertThatThrownBy(() -> new Subject(user1, user2))
                .hasMessage("2 principals of class io.kroxylicious.proxy.authentication.User were found, "
                        + "but class io.kroxylicious.proxy.authentication.User is annotated with interface "
                        + "io.kroxylicious.proxy.authentication.Unique");

        Assertions.assertThatThrownBy(() -> new Subject(user1, unique, unique2))
                .hasMessage("2 principals of class io.kroxylicious.proxy.authentication.FakeUniquePrincipal were found, "
                        + "but class io.kroxylicious.proxy.authentication.FakeUniquePrincipal is annotated with interface "
                        + "io.kroxylicious.proxy.authentication.Unique");
    }

    @Test
    void canExtractUniquePrincipals() { // but not eventually
        Subject subject = new Subject(user1, unique);
        Assertions.assertThat(subject.uniquePrincipalOfType(User.class)).hasValue(user1);
        Assertions.assertThat(subject.uniquePrincipalOfType(FakeUniquePrincipal.class)).hasValue(unique);
        Subject subject2 = new Subject(user1);
        Assertions.assertThat(subject2.uniquePrincipalOfType(FakeUniquePrincipal.class)).isEmpty();
        Assertions.assertThat(Subject.anonymous().uniquePrincipalOfType(FakeUniquePrincipal.class)).isEmpty();
    }

    @Test
    void throwIaeWhenUsingNonUniqueClassWithUniqueExractor() { // but not eventually
        Subject subject = new Subject(user1, unique);
        Assertions.assertThatThrownBy(() -> subject.uniquePrincipalOfType(FakeMultiplePrincipal.class))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("class io.kroxylicious.proxy.authentication.FakeMultiplePrincipal is not annotated with interface io.kroxylicious.proxy.authentication.Unique");

    }

    @Test
    void canExtractPrincipals() { // but not eventually
        Subject subject = new Subject(user1, unique, foo, bar);
        Assertions.assertThat(subject.allPrincipalsOfType(User.class)).isEqualTo(Set.of(user1));
        Assertions.assertThat(subject.allPrincipalsOfType(FakeUniquePrincipal.class)).isEqualTo(Set.of(unique));
        Assertions.assertThat(subject.allPrincipalsOfType(FakeMultiplePrincipal.class)).isEqualTo(Set.of(foo, bar));
        Subject subject2 = new Subject(user1);
        Assertions.assertThat(subject2.allPrincipalsOfType(FakeUniquePrincipal.class)).isEmpty();
        Assertions.assertThat(Subject.anonymous().allPrincipalsOfType(FakeUniquePrincipal.class)).isEmpty();
    }
}