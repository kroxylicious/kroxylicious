/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration;

import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShellUtilsTest {

    private static final String NO_SUCH_CMD = "no_such_cmd_xyz_kroxy";
    private static final Predicate<Stream<String>> ALWAYS_VALID = lines -> true;
    private static final Predicate<Stream<String>> ALWAYS_INVALID = lines -> false;

    // execValidate

    @Test
    void execValidateShouldReturnTrueWhenCommandSucceedsAndValidatorsPass() {
        var result = ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "true");
        assertThat(result).isTrue();
    }

    @Test
    void execValidateShouldReturnFalseWhenStdoutValidatorRejects() {
        var result = ShellUtils.execValidate(ALWAYS_INVALID, ALWAYS_VALID, "true");
        assertThat(result).isFalse();
    }

    @Test
    void execValidateShouldReturnFalseWhenStderrValidatorRejects() {
        var result = ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_INVALID, "true");
        assertThat(result).isFalse();
    }

    @Test
    void execValidateShouldReturnFalseWhenBothValidatorsReject() {
        var result = ShellUtils.execValidate(ALWAYS_INVALID, ALWAYS_INVALID, "true");
        assertThat(result).isFalse();
    }

    @Test
    void execValidateShouldThrowOnNonZeroExit() {
        assertThatThrownBy(() -> ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "false"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void execValidateShouldThrowOnNonExistentCommand() {
        assertThatThrownBy(() -> ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, NO_SUCH_CMD))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void execValidateShouldPassStdoutContentToValidator() {
        var result = ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("hello")),
                ALWAYS_VALID,
                "echo", "hello");
        assertThat(result).isTrue();
    }

    @Test
    void execValidateShouldThrowOnTimeout() {
        assertThatThrownBy(() -> ShellUtils.execValidate(
                ALWAYS_VALID, ALWAYS_VALID, 1, TimeUnit.MILLISECONDS, "sleep", "60"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("did not complete within timeout");
    }

    @Test
    void execValidateShouldPassStderrContentToValidator() {
        var result = ShellUtils.execValidate(
                ALWAYS_VALID,
                lines -> lines.anyMatch(line -> line.contains("err")),
                "sh", "-c", "echo err >&2");
        assertThat(result).isTrue();
    }

    // exec

    @Test
    void execShouldNotThrowOnSuccess() {
        assertThatCode(() -> ShellUtils.exec("true"))
                .doesNotThrowAnyException();
    }

    @Test
    void execShouldThrowOnFailure() {
        assertThatThrownBy(() -> ShellUtils.exec("false"))
                .isInstanceOf(AssertionError.class);
    }

    // isToolOnPath

    @Test
    void isToolOnPathShouldReturnTrueForExistingTool() {
        assertThat(ShellUtils.isToolOnPath("true")).isTrue();
    }

    @Test
    void isToolOnPathShouldReturnFalseForMissingTool() {
        assertThat(ShellUtils.isToolOnPath(NO_SUCH_CMD)).isFalse();
    }

    // validateToolsOnPath

    @Test
    void validateToolsOnPathShouldReturnTrueWhenAllPresent() {
        assertThat(ShellUtils.validateToolsOnPath("true", "echo")).isTrue();
    }

    @Test
    void validateToolsOnPathShouldReturnFalseWhenOneMissing() {
        assertThat(ShellUtils.validateToolsOnPath("true", NO_SUCH_CMD)).isFalse();
    }

    @Test
    void validateToolsOnPathShouldReturnTrueForEmptyArgs() {
        assertThat(ShellUtils.validateToolsOnPath()).isTrue();
    }
}
