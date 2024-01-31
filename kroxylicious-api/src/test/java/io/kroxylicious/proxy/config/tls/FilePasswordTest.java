/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
        Function<String, PasswordProvider> filePassword = FilePassword::new;
        Function<String, PasswordProvider> filePasswordPath = FilePasswordFilePath::new;
        return Stream.of(
                Arguments.of(filePassword, "mypassword", "mypassword"),
                Arguments.of(filePassword, "mypassword\n", "mypassword"),
                Arguments.of(filePassword, "mypassword\nignores\nadditional lines", "mypassword"),
                Arguments.of(filePasswordPath, "mypassword", "mypassword"),
                Arguments.of(filePasswordPath, "mypassword\n", "mypassword"),
                Arguments.of(filePasswordPath, "mypassword\nignores\nadditional lines", "mypassword"));
    }

    @ParameterizedTest
    @MethodSource
    void readPassword(Function<String, PasswordProvider> providerFunc, String input, String expected) throws Exception {
        Files.writeString(file.toPath(), input);
        var provider = providerFunc.apply(file.getAbsolutePath());
        assertThat(provider)
                .extracting(PasswordProvider::getProvidedPassword)
                .isEqualTo(expected);
    }

    static List<Function<String, PasswordProvider>> providers() {
        return List.of(FilePassword::new, FilePasswordFilePath::new);
    }

    @ParameterizedTest
    @MethodSource("providers")
    void toStringDoesNotLeakPassword(Function<String, PasswordProvider> providerFunc) throws Exception {
        var password = "mypassword";
        Files.writeString(file.toPath(), password);
        var provider = providerFunc.apply(file.getAbsolutePath());
        assertThat(provider)
                .extracting(Object::toString)
                .doesNotHave(new Condition<>(s -> s.contains(password), "contains password"));
    }

    @ParameterizedTest
    @MethodSource("providers")
    void passwordFileNotFound(Function<String, PasswordProvider> providerFunc) {
        assertThat(file.delete()).isTrue();

        String path = file.getAbsolutePath();
        var provider = providerFunc.apply(path);
        assertThatThrownBy(provider::getProvidedPassword)
                .hasMessageContaining(path)
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }
}
