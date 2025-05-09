/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolsTest {
    @Test
    void allow() {
        // Given
        var protocols = new ProtocolsBuilder().addToAllow("allow1", "allow2").addToAllow("allow3").build();

        // When
        var allowed = protocols.getAllow();

        // Then
        assertThat(allowed)
                .containsExactly("allow1", "allow2", "allow3");
    }

    @Test
    void deny() {
        // Given
        var protocols = new ProtocolsBuilder().addToDeny("deny1", "deny2").addToDeny("deny3").build();

        // When
        var denied = protocols.getDeny();

        // Then
        assertThat(denied)
                .containsExactly("deny1", "deny2", "deny3");
    }

    @Test
    void shouldReturnDifferentBuilder() {
        // Given
        var originalBuilder = new ProtocolsBuilder();
        var protocols = originalBuilder.build();

        // When
        ProtocolsBuilder actualBuilder = protocols.edit();

        // Then
        assertThat(actualBuilder)
                .isNotNull()
                .isInstanceOf(ProtocolsBuilder.class)
                .isNotSameAs(originalBuilder);
    }

    @Test
    void shouldAllowEditingOfExistingObject() {
        // Given
        var original = new Protocols();
        original.setAllow(List.of("allow1"));
        original.setDeny(List.of("deny"));

        // When
        ProtocolsBuilder builder = original.edit();
        builder.addToAllow("allow2")
                .withDeny("overwritten");
        var replacement = builder.build();

        // Then
        assertThat(replacement)
                .satisfies(r -> {
                    assertThat(r.getAllow()).containsExactly("allow1", "allow2");
                    assertThat(r.getDeny()).containsExactly("overwritten");
                });
    }

}
