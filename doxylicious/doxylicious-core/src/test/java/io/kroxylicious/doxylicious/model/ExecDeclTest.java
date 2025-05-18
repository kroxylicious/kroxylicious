/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecDeclTest {

    @Test
    void shouldParseDurationInMilliseconds() {
        assertThat(ExecDecl.parseDuration("42ms")).isEqualTo(Duration.ofMillis(42));
    }

    @Test
    void shouldParseDurationInSeconds() {
        assertThat(ExecDecl.parseDuration("42s")).isEqualTo(Duration.ofSeconds(42));
    }

    @Test
    void shouldParseDurationInMinutes() {
        assertThat(ExecDecl.parseDuration("42m")).isEqualTo(Duration.ofMinutes(42));
    }

    @Test
    void shouldParseDurationInHours() {
        assertThat(ExecDecl.parseDuration("42h")).isEqualTo(Duration.ofHours(42));
    }

    @Test
    void shouldRequireOneOfCommandOrArgs() {
        List<String> empty = List.of();
        assertThatThrownBy(() -> new ExecDecl(null, null, "", empty, null, null, null, null, null)).isExactlyInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExecDecl(null, null, null, null, null, null, null, null, null)).isExactlyInstanceOf(IllegalArgumentException.class);

    }

    @Test
    void shouldParseYamlObjectWithArgs() throws JsonProcessingException {
        var decl = new YAMLMapper().readValue("""
                args: [x, y, z]
                """, ExecDecl.class);
        assertThat(decl.args()).isEqualTo(List.of("x", "y", "z"));
    }

    @Test
    void shouldParseYamlObjectWithCommand() throws JsonProcessingException {
        var decl = new YAMLMapper().readValue("""
                command: x y z
                """, ExecDecl.class);
        assertThat(decl.args()).isEqualTo(List.of("x", "y", "z"));
    }

}