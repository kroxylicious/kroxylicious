/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common.tls;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CipherSuitesTest {
    @Test
    void allow() {
        // Given
        var cipherSuites = new CipherSuitesBuilder().addToAllow("allow1", "allow2").addToAllow("allow3").build();

        // When
        var allowed = cipherSuites.getAllow();

        // Then
        assertThat(allowed)
                .containsExactly("allow1", "allow2", "allow3");
    }

    @Test
    void deny() {
        // Given
        var cipherSuites = new CipherSuitesBuilder().addToDeny("deny1", "deny2").addToDeny("deny3").build();

        // When
        var denied = cipherSuites.getDeny();

        // Then
        assertThat(denied)
                .containsExactly("deny1", "deny2", "deny3");
    }

    @Test
    void shouldReturnDifferentBuilder() {
        // Given
        var originalBuilder = new CipherSuitesBuilder();
        var cipherSuites = originalBuilder.build();

        // When
        CipherSuitesBuilder actualBuilder = cipherSuites.edit();

        // Then
        assertThat(actualBuilder)
                .isNotNull()
                .isInstanceOf(CipherSuitesBuilder.class)
                .isNotSameAs(originalBuilder);
    }

    @Test
    void shouldAllowEditingOfExistingObject() {
        // Given
        var original = new CipherSuites();
        original.setAllow(List.of("allow1"));
        original.setDeny(List.of("deny"));

        // When
        CipherSuitesBuilder builder = original.edit();
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