/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ActionTest {

    @Test
    void resourceTypeClassReturnsDeclaredEnumTypeForConstantWithAnonymousBody() {
        // Given
        // READ has an anonymous body, so READ.getClass() returns an anonymous subclass rather than
        // ResourceTypeWithAnonymousConstantBody.class.
        var action = new Action(ResourceTypeWithAnonymousConstantBody.READ, "my-resource");

        // When
        var resourceTypeClass = action.resourceTypeClass();

        // Then
        assertThat(resourceTypeClass).isEqualTo(ResourceTypeWithAnonymousConstantBody.class);
    }

    private enum ResourceTypeWithAnonymousConstantBody implements ResourceType<ResourceTypeWithAnonymousConstantBody> {
        READ {
            @Override
            public Set<ResourceTypeWithAnonymousConstantBody> implies() {
                return Set.of(DESCRIBE);
            }
        },
        WRITE,
        DESCRIBE;

        @Override
        public Set<ResourceTypeWithAnonymousConstantBody> implies() {
            return Set.of();
        }
    }
}
