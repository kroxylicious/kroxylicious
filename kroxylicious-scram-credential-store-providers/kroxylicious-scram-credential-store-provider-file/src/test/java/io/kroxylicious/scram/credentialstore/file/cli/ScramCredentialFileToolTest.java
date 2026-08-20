/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.KeyStoreException;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import io.kroxylicious.scram.credentialstore.file.CredentialValidationException;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileManager;

import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for {@link ScramCredentialFileTool} CLI logic.
 */
class ScramCredentialFileToolTest {

    // ===== getPassword() tests =====

    @Test
    void getPasswordShouldThrowWhenOptionProvidedWithoutUnlock() {
        // Given
        var err = new PrintWriter(new StringWriter());

        // When/Then
        assertThatThrownBy(() -> ScramCredentialFileTool.getPassword("secret", false, "Password", false, err))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Password options are disabled by default for security")
                .hasMessageContaining("--unlock-insecure-options");
    }

    @Test
    void getPasswordShouldReturnValueWhenOptionProvidedWithUnlock() {
        // Given
        var err = new PrintWriter(new StringWriter());

        // When
        String result = ScramCredentialFileTool.getPassword("my-secret-password", true, "Password", false, err);

        // Then
        assertThat(result).isEqualTo("my-secret-password");
    }

    @Test
    void getPasswordShouldPrintSecurityWarningWhenUnlocked() {
        // Given
        var errWriter = new StringWriter();

        // When
        ScramCredentialFileTool.getPassword("my-secret-password", true, "Password", false,
                new PrintWriter(errWriter, true));

        // Then
        assertThat(errWriter.toString())
                .contains("SECURITY WARNING")
                .contains("NOT RECOMMENDED")
                .contains("Process listings")
                .contains("Shell history")
                .contains("System audit logs");
    }

    @Test
    void getPasswordShouldThrowWhenConsoleReturnsNull() {
        // Given
        var err = new PrintWriter(new StringWriter());

        try (MockedStatic<ScramCredentialFileTool> mocked = mockStatic(ScramCredentialFileTool.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> ScramCredentialFileTool.readPasswordFromConsole(anyString()))
                    .thenReturn(null);

            // When/Then
            assertThatThrownBy(() -> ScramCredentialFileTool.getPassword(null, false, "Password", false, err))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Cannot read password interactively")
                    .hasMessageContaining("no console available");
        }
    }

    @Test
    void getPasswordShouldReturnInteractivePasswordWithoutConfirmation() {
        // Given
        var err = new PrintWriter(new StringWriter());

        try (MockedStatic<ScramCredentialFileTool> mocked = mockStatic(ScramCredentialFileTool.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> ScramCredentialFileTool.readPasswordFromConsole(anyString()))
                    .thenReturn("interactive-password");

            // When
            String result = ScramCredentialFileTool.getPassword(null, false, "Password", false, err);

            // Then
            assertThat(result).isEqualTo("interactive-password");
        }
    }

    @Test
    void getPasswordShouldReturnInteractivePasswordWhenConfirmationMatches() {
        // Given
        var err = new PrintWriter(new StringWriter());

        try (MockedStatic<ScramCredentialFileTool> mocked = mockStatic(ScramCredentialFileTool.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> ScramCredentialFileTool.readPasswordFromConsole(anyString()))
                    .thenReturn("matching-password");

            // When
            String result = ScramCredentialFileTool.getPassword(null, false, "Password", true, err);

            // Then
            assertThat(result).isEqualTo("matching-password");
        }
    }

    @Test
    void getPasswordShouldThrowWhenConfirmationDoesNotMatch() {
        // Given
        var err = new PrintWriter(new StringWriter());

        try (MockedStatic<ScramCredentialFileTool> mocked = mockStatic(ScramCredentialFileTool.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> ScramCredentialFileTool.readPasswordFromConsole(anyString()))
                    .thenReturn("first-password", "different-password");

            // When/Then
            assertThatThrownBy(() -> ScramCredentialFileTool.getPassword(null, false, "Password", true, err))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Passwords do not match");
        }
    }

    // ===== ScramMechanismType tests =====

    @Test
    void scramSha256TypeShouldMapToKafkaMechanism() {
        assertThat(ScramCredentialFileTool.ScramMechanismType.SCRAM_SHA_256.toScramMechanism())
                .isEqualTo(ScramMechanism.SCRAM_SHA_256);
    }

    @Test
    void scramSha512TypeShouldMapToKafkaMechanism() {
        assertThat(ScramCredentialFileTool.ScramMechanismType.SCRAM_SHA_512.toScramMechanism())
                .isEqualTo(ScramMechanism.SCRAM_SHA_512);
    }

    // ===== Command KeyStoreException handling (exit code 1) =====

    @Test
    void createCommandShouldReturnExitCode1OnKeystoreException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new KeyStoreException("simulated error")).when(mock).createKeyStore(any(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "create",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("Failed to create KeyStore")
                    .contains("simulated error");
        }
    }

    @Test
    void createCommandShouldReturnExitCode1OnCredentialValidationException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new CredentialValidationException("Password too short")).when(mock).createKeyStore(any(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "create",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr()).contains("Password too short");
        }
    }

    @Test
    void addUserCommandShouldReturnExitCode1OnCredentialValidationException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new CredentialValidationException("User password too short")).when(mock).addUser(any(), anyString(), anyString(), anyString(), any(), anyInt());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "add-user",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure",
                    "-u", "alice",
                    "-w", "user-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr()).contains("User password too short");
        }
    }

    @Test
    void addUserCommandShouldReturnExitCode1OnKeystoreException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new KeyStoreException("simulated error")).when(mock).addUser(any(), anyString(), anyString(), anyString(), any(), anyInt());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "add-user",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure",
                    "-u", "alice",
                    "-w", "user-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("Failed to add user")
                    .contains("simulated error");
        }
    }

    @Test
    void removeUserCommandShouldReturnExitCode1OnKeystoreException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new KeyStoreException("simulated error")).when(mock).removeUser(any(), anyString(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "remove-user",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure",
                    "-u", "alice");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("Failed to remove user")
                    .contains("simulated error");
        }
    }

    @Test
    void updatePasswordCommandShouldReturnExitCode1OnKeystoreException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new KeyStoreException("simulated error")).when(mock).updatePassword(any(), anyString(), anyString(), anyString(), any(), anyInt());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "update-password",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure",
                    "-u", "alice",
                    "-w", "new-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("Failed to update password")
                    .contains("simulated error");
        }
    }

    @Test
    void listUsersCommandShouldReturnExitCode1OnKeystoreException() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new KeyStoreException("simulated error")).when(mock).listCredentials(any(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "list-users",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("Failed to list users")
                    .contains("simulated error");
        }
    }

    // ===== formatError() exception chain formatting =====

    @Test
    void commandShouldFormatExceptionWithCauseChain() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            var rootCause = new IOException("disk full");
            var intermediateCause = new RuntimeException("write failed", rootCause);
            doThrow(new KeyStoreException("store error", intermediateCause)).when(mock).createKeyStore(any(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "create",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("Failed to create KeyStore")
                    .contains("store error")
                    .contains("write failed")
                    .contains("disk full");
        }
    }

    @Test
    void commandShouldFormatExceptionWithNullMessage() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            doThrow(new KeyStoreException((String) null)).when(mock).createKeyStore(any(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "create",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr()).contains("Failed to create KeyStore");
        }
    }

    @Test
    void commandShouldStopFormattingCauseChainAtNullMessage() {
        try (MockedConstruction<ScramCredentialFileManager> ignored = mockConstruction(ScramCredentialFileManager.class, (mock, context) -> {
            var deepCause = new RuntimeException("deep cause");
            var causeWithNullMsg = new RuntimeException((String) null, deepCause);
            doThrow(new KeyStoreException("top level", causeWithNullMsg)).when(mock).createKeyStore(any(), anyString());
        })) {
            // When
            var result = executeCommand("--unlock-insecure-options", "create",
                    "-k", "/tmp/test.p12",
                    "-p", "test-password-secure");

            // Then
            assertThat(result.exitCode()).isEqualTo(1);
            assertThat(result.stderr())
                    .contains("top level")
                    .doesNotContain("deep cause");
        }
    }

    // ===== Command IllegalStateException handling (exit code 2) =====

    @Test
    void addUserCommandShouldReturnExitCode2WhenPasswordOptionNotUnlocked() {
        // When
        var result = executeCommand("add-user",
                "-k", "/tmp/test.p12",
                "-p", "test-password",
                "-u", "alice",
                "-w", "user-password");

        // Then
        assertThat(result.exitCode()).isEqualTo(2);
        assertThat(result.stderr()).contains("Password options are disabled by default");
    }

    @Test
    void removeUserCommandShouldReturnExitCode2WhenPasswordOptionNotUnlocked() {
        // When
        var result = executeCommand("remove-user",
                "-k", "/tmp/test.p12",
                "-p", "test-password",
                "-u", "alice");

        // Then
        assertThat(result.exitCode()).isEqualTo(2);
        assertThat(result.stderr()).contains("Password options are disabled by default");
    }

    @Test
    void updatePasswordCommandShouldReturnExitCode2WhenPasswordOptionNotUnlocked() {
        // When
        var result = executeCommand("update-password",
                "-k", "/tmp/test.p12",
                "-p", "test-password",
                "-u", "alice",
                "-w", "new-password");

        // Then
        assertThat(result.exitCode()).isEqualTo(2);
        assertThat(result.stderr()).contains("Password options are disabled by default");
    }

    @Test
    void listUsersCommandShouldReturnExitCode2WhenPasswordOptionNotUnlocked() {
        // When
        var result = executeCommand("list-users",
                "-k", "/tmp/test.p12",
                "-p", "test-password");

        // Then
        assertThat(result.exitCode()).isEqualTo(2);
        assertThat(result.stderr()).contains("Password options are disabled by default");
    }

    // ===== Helpers =====

    private CommandResult executeCommand(String... args) {
        var stdout = new StringWriter();
        var stderr = new StringWriter();

        var cmd = new CommandLine(new ScramCredentialFileTool())
                .setOut(new PrintWriter(stdout))
                .setErr(new PrintWriter(stderr));

        int exitCode = cmd.execute(args);

        return new CommandResult(exitCode, stdout.toString(), stderr.toString());
    }

    private record CommandResult(int exitCode, String stdout, String stderr) {}
}
