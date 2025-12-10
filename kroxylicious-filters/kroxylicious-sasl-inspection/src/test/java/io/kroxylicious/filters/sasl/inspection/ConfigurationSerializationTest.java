/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.kroxylicious.proxy.config.ConfigParser;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationSerializationTest {

    @Test
    void deserializeEmpty() throws IOException {
        Config config = parseConfig("{}");
        assertThat(config.enabledMechanisms()).isNull();
        assertThat(config.requireAuthentication()).isNull();
        assertThat(config.subjectBuilderConfig()).isNull();
        assertThat(config.subjectBuilder()).isNull();
    }

    @Test
    void deserializeEnabledMechanisms() throws IOException {
        String configYaml = """
                enabledMechanisms:
                  - PLAIN
                """;
        Config config = parseConfig(configYaml);
        assertThat(config.enabledMechanisms()).containsExactly("PLAIN");
    }

    @CsvSource({ "true", "false" })
    @ParameterizedTest
    void deserializeRequireAuthentication(boolean requireAuthentication) throws IOException {
        String configYaml = """
                requireAuthentication: %s
                """.formatted(requireAuthentication);
        Config config = parseConfig(configYaml);
        assertThat(config.requireAuthentication()).isEqualTo(requireAuthentication);
    }

    @Test
    void deserializeSubjectBuilder() throws IOException {
        String configYaml = """
                subjectBuilder: MockSubjectBuilderService
                """;
        Config config = parseConfig(configYaml);
        assertThat(config.subjectBuilder()).isEqualTo("MockSubjectBuilderService");
    }

    // covers the plugin annotations on io.kroxylicious.filters.sasl.inspection.Config
    @Test
    void deserializeSubjectBuilderConfig() throws IOException {
        String configYaml = """
                subjectBuilder: MockSubjectBuilderService
                subjectBuilderConfig:
                  arbitraryProperty: foo
                """;
        Config config = parseConfig(configYaml);
        assertThat(config.subjectBuilderConfig()).isInstanceOf(MockSubjectBuilderService.Config.class);
    }

    private static Config parseConfig(String input) throws IOException {
        return ConfigParser.createObjectMapper().reader().readValue(input, Config.class);
    }
}
