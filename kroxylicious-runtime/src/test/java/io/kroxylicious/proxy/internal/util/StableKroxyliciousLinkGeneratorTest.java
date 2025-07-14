/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.VersionInfo;

import static org.assertj.core.api.Assertions.assertThat;

class StableKroxyliciousLinkGeneratorTest {

    @Test
    void shouldGenerateLinkToErrorNamespace() {
        // Given

        // When
        String errorLink = StableKroxyliciousLinkGenerator.errorLink("suspectedTls");

        // Then
        assertThat(errorLink).contains("/redirects/errors/");
    }

    @Test
    void shouldGenerateLinkWithVersionSpecifier() {
        // Given

        // When
        String errorLink = StableKroxyliciousLinkGenerator.errorLink("suspectedTls");

        // Then
        assertThat(errorLink).contains(VersionInfo.VERSION_INFO.version());
    }
}