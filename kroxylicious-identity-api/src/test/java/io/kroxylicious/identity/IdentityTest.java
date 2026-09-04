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

class IdentityTest {

    @Test
    void anonymousShouldBeEmpty() {
        Identity identity = Identity.anonymous();
        assertThat(identity.isAnonymous()).isTrue();
        assertThat(identity.principals()).isEmpty();
    }

    @Test
    void defaultsShouldOperateOverPrincipals() {
        var user = new SingularUser("alice");
        var role = new Role("admin");
        Identity identity = new Subject(Set.of(user, role));

        assertThat(identity.uniquePrincipalOfType(SingularUser.class)).contains(user);
        assertThat(identity.allPrincipalsOfType(Role.class)).containsExactly(role);
        assertThat(identity.isAnonymous()).isFalse();
    }

    @Test
    void uniquePrincipalOfTypeShouldRejectNonSingularType() {
        Identity identity = new Subject(Set.of(new Role("admin")));
        assertThatThrownBy(() -> identity.uniquePrincipalOfType(Role.class))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
