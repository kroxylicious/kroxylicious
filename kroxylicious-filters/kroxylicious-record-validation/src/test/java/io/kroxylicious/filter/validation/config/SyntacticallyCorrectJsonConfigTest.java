/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SyntacticallyCorrectJsonConfigTest {

    @Test
    void equalConfigsShouldHaveEqualHashCodes() {
        // Given
        var config = new SyntacticallyCorrectJsonConfig(true);
        var other = new SyntacticallyCorrectJsonConfig(true);

        // When
        int hashCode = config.hashCode();

        // Then
        assertThat(config).isEqualTo(other);
        assertThat(hashCode).isEqualTo(other.hashCode());
    }

    @Test
    void configsWithDifferentValidateObjectKeysUniqueShouldHaveDifferentHashCodes() {
        // Given
        var config = new SyntacticallyCorrectJsonConfig(true);
        var other = new SyntacticallyCorrectJsonConfig(false);

        // When
        int hashCode = config.hashCode();

        // Then
        assertThat(config).isNotEqualTo(other);
        assertThat(hashCode).isNotEqualTo(other.hashCode());
    }

    @Test
    void nullValidateObjectKeysUniqueShouldBeEquivalentToFalse() {
        // Given
        var config = new SyntacticallyCorrectJsonConfig(null);
        var other = new SyntacticallyCorrectJsonConfig(false);

        // When
        int hashCode = config.hashCode();

        // Then
        assertThat(config).isEqualTo(other);
        assertThat(hashCode).isEqualTo(other.hashCode());
    }
}
