/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StableKroxyliciousLinkGeneratorTest {

    private StableKroxyliciousLinkGenerator stableKroxyliciousLinkGenerator;

    @BeforeEach
    void setUp() {
        stableKroxyliciousLinkGenerator = new StableKroxyliciousLinkGenerator(() -> new ByteArrayInputStream("""
                errors.clientTls=https://example.com/redirect/errors/
                alternative.clientTls=https://example.com/alternative
                """.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void shouldThrowExceptionIfSlugIsNotInGivenNamespace() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> stableKroxyliciousLinkGenerator.errorLink("unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No link found for errors.unknown");
    }

    @Test
    void shouldLoadLinkFromErrorNamespace() {
        // Given

        // When
        String errorLink = stableKroxyliciousLinkGenerator.errorLink("clientTls");

        // Then
        assertThat(errorLink).isEqualTo("https://example.com/redirect/errors/");
    }
}