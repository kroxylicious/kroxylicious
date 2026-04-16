/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigSchemaValidatorTest {

    private ConfigSchemaValidator validator;

    @BeforeEach
    void setUp() {
        validator = new ConfigSchemaValidator(
                (YAMLMapper) new YAMLMapper().findAndRegisterModules());
    }

    @Test
    void validConfigPassesValidation() {
        // RequiresConfigFactory/v1alpha1 schema requires {foo: string}
        assertThatCode(() -> validator.validateIfSchemaAvailable(
                "RequiresConfigFactory", "v1alpha1", "my-filter",
                Map.of("foo", "hello")))
                .doesNotThrowAnyException();
    }

    @Test
    void missingRequiredFieldRejected() {
        assertThatThrownBy(() -> validator.validateIfSchemaAvailable(
                "RequiresConfigFactory", "v1alpha1", "my-filter",
                Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-filter")
                .hasMessageContaining("RequiresConfigFactory")
                .hasMessageContaining("foo");
    }

    @Test
    void wrongTypeRejected() {
        assertThatThrownBy(() -> validator.validateIfSchemaAvailable(
                "RequiresConfigFactory", "v1alpha1", "my-filter",
                Map.of("foo", 123)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-filter")
                .hasMessageContaining("string");
    }

    @Test
    void additionalPropertyRejected() {
        assertThatThrownBy(() -> validator.validateIfSchemaAvailable(
                "RequiresConfigFactory", "v1alpha1", "my-filter",
                Map.of("foo", "hello", "bar", "unexpected")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-filter")
                .hasMessageContaining("bar");
    }

    @Test
    void noSchemaOnClasspathSkipsValidation() {
        // "NoSuchPlugin" has no schema — should not throw
        assertThatCode(() -> validator.validateIfSchemaAvailable(
                "NoSuchPlugin", "v99", "inst",
                Map.of("anything", "goes")))
                .doesNotThrowAnyException();
    }

    @Test
    void fullyQualifiedTypeNameResolvesToSimpleName() {
        // Should find schema for RequiresConfigFactory even with FQCN
        assertThatCode(() -> validator.validateIfSchemaAvailable(
                "com.example.RequiresConfigFactory", "v1alpha1", "my-filter",
                Map.of("foo", "hello")))
                .doesNotThrowAnyException();
    }

    @Test
    void nullConfigValidatedAgainstSchema() {
        // null config with required field should fail
        assertThatThrownBy(() -> validator.validateIfSchemaAvailable(
                "RequiresConfigFactory", "v1alpha1", "my-filter",
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-filter");
    }
}
