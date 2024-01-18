/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilePasswordTest {

    private File file;

    @BeforeEach
    void setUp() throws Exception {
        file = File.createTempFile("password", "txt");
        file.deleteOnExit();
    }

    @AfterEach
    void afterEach() {
        if (file != null) {
            var ignored = file.delete();
        }
    }

    static Stream<Arguments> readPassword() {
        return Stream.of(
                Arguments.of("mypassword", "mypassword"),
                Arguments.of("mypassword\n", "mypassword"),
                Arguments.of("mypassword\nignores\nadditional lines", "mypassword"));
    }

    @ParameterizedTest
    @MethodSource
    void readPassword(String input, String expected) throws Exception {
        Files.writeString(file.toPath(), input);
        var provider = new FilePassword(file.getAbsolutePath());
        assertThat(provider)
                .extracting(FilePassword::getProvidedPassword)
                .isEqualTo(expected);
    }

    @Test
    void toStringDoesNotLeakPassword() throws Exception {
        var password = "mypassword";
        Files.writeString(file.toPath(), password);
        var provider = new FilePassword(file.getAbsolutePath());
        assertThat(provider)
                .extracting(FilePassword::toString)
                .doesNotHave(new Condition<>(s -> s.contains(password), "contains password"));
    }

    @Test
    void passwordFileNotFound() {
        assertThat(file.delete()).isTrue();

        var provider = new FilePassword(file.getAbsolutePath());
        assertThatThrownBy(provider::getProvidedPassword)
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    @Test

    void nullFileTreatedAsNullPassword() {
        assertThat(new FilePassword(null))
                .extracting(FilePassword::getProvidedPassword)
                .isNull();
    }
}
