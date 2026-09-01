/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingularPrincipalsTest {

    @Test
    void shouldRecogniseDirectlyAnnotatedTypeAsSingular() {
        assertThat(SingularPrincipals.isSingular(SingularUser.class)).isTrue();
    }

    @Test
    void shouldRecogniseMetaAnnotatedTypeAsSingular() {
        assertThat(SingularPrincipals.isSingular(MetaSingularUser.class)).isTrue();
    }

    @Test
    void shouldNotRecogniseUnannotatedTypeAsSingular() {
        assertThat(SingularPrincipals.isSingular(Role.class)).isFalse();
    }

    @Test
    void shouldAllowOneInstanceOfEachDistinctSingularType() {
        Set<Principal> principals = Set.of(new SingularUser("alice"), new MetaSingularUser("alice"));
        assertThatNoException().isThrownBy(() -> SingularPrincipals.validateUniqueness(principals));
    }

    @Test
    void shouldRejectMultipleInstancesOfSameSingularType() {
        Set<Principal> principals = Set.of(new SingularUser("alice"), new SingularUser("bob"));
        assertThatThrownBy(() -> SingularPrincipals.validateUniqueness(principals))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("singular principal type");
    }

    @Test
    void shouldAllowMultipleInstancesOfNonSingularType() {
        Set<Principal> principals = Set.of(new Role("admin"), new Role("developer"));
        assertThatNoException().isThrownBy(() -> SingularPrincipals.validateUniqueness(principals));
    }
}
