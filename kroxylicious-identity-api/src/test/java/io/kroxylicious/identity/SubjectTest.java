/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubjectTest {

    @Test
    void anonymousShouldHaveNoPrincipals() {
        assertThat(Subject.anonymous().principals()).isEmpty();
        assertThat(Subject.anonymous().isAnonymous()).isTrue();
    }

    @Test
    void shouldExposePrincipals() {
        var role = new Role("admin");
        var subject = new Subject(Set.of(role));

        assertThat(subject.principals()).singleElement().isEqualTo(role);
        assertThat(subject.isAnonymous()).isFalse();
    }

    @Test
    void shouldRejectMultipleInstancesOfSingularType() {
        assertThatThrownBy(() -> new Subject(Set.of(new SingularUser("alice"), new SingularUser("bob"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAllowMultipleNonSingularPrincipals() {
        var subject = new Subject(Set.of(new Role("admin"), new Role("developer")));
        assertThat(subject.allPrincipalsOfType(Role.class)).hasSize(2);
    }

    @Test
    void shouldNotRequireAUserPrincipal() {
        // unlike the proxy's deprecated Subject, the general-purpose Subject imposes no User invariant
        var subject = new Subject(Set.of(new Role("admin")));
        assertThat(subject.principals()).hasSize(1);
    }

    @Test
    void uniquePrincipalOfTypeShouldReturnSingleInstance() {
        var user = new SingularUser("alice");
        var subject = new Subject(Set.of(user));
        assertThat(subject.uniquePrincipalOfType(SingularUser.class)).contains(user);
    }

    @Test
    void uniquePrincipalOfTypeShouldRejectNonSingularType() {
        var subject = new Subject(Set.of(new Role("admin")));
        assertThatThrownBy(() -> subject.uniquePrincipalOfType(Role.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldMakeADefensiveCopyOfPrincipals() {
        var mutable = new java.util.HashSet<Principal>();
        mutable.add(new Role("admin"));
        var subject = new Subject(mutable);
        mutable.add(new Role("developer"));
        assertThat(subject.principals()).hasSize(1);
    }
}
