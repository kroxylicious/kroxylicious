/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class UserFactoryTest {

    @Test
    void createsUserPrincipalWithGivenName() {
        // Given
        UserFactory factory = new UserFactory();

        // When
        User user = factory.newPrincipal("alice");

        // Then
        Assertions.assertThat(user).isEqualTo(new User("alice"));
    }
}
