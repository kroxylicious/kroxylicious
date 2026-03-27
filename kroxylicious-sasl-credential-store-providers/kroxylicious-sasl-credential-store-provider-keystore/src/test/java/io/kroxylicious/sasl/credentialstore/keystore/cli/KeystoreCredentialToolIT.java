/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link KeystoreCredentialTool} CLI.
 */
class KeystoreCredentialToolIT {

    private static final String KEYSTORE_PASSWORD = "test-password";

    @Test
    void shouldCreateKeystoreAndListUsers(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        // Create keystore
        var createResult = executeCommand("--unlock-insecure-options", "create",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-t", "PKCS12");

        assertThat(createResult.exitCode()).isZero();
        assertThat(createResult.stdout()).contains("KeyStore created successfully");
        assertThat(keystorePath).exists();

        // List users (should be empty)
        var listResult = executeCommand("--unlock-insecure-options", "list-users",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(listResult.exitCode()).isZero();
        assertThat(listResult.stdout()).contains("No users found");
    }

    @Test
    void shouldAddUserAndListUsers(@TempDir Path tempDir) throws Exception {
        Path keystorePath = tempDir.resolve("credentials.p12");

        // Create keystore first
        executeCommand("--unlock-insecure-options", "create",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        // Add user
        var addResult = executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "alice-secret");

        assertThat(addResult.exitCode()).isZero();
        assertThat(addResult.stdout()).contains("User 'alice' added successfully");

        // List users
        var listResult = executeCommand("--unlock-insecure-options", "list-users",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(listResult.exitCode()).isZero();
        assertThat(listResult.stdout())
                .contains("Users in KeyStore (1):")
                .contains("alice");
    }

    @Test
    void shouldAddMultipleUsersAndListInOrder(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);

        // Add users in non-alphabetical order
        executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "zebra",
                "-w", "password");

        executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "password");

        executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "mike",
                "-w", "password");

        // List should be sorted
        var listResult = executeCommand("--unlock-insecure-options", "list-users",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(listResult.exitCode()).isZero();
        assertThat(listResult.stdout()).contains("Users in KeyStore (3):");

        // Extract lines and verify order
        var lines = listResult.stdout().lines().toList();
        var aliceIndex = findLineIndex(lines, "alice");
        var mikeIndex = findLineIndex(lines, "mike");
        var zebraIndex = findLineIndex(lines, "zebra");

        assertThat(aliceIndex).isLessThan(mikeIndex);
        assertThat(mikeIndex).isLessThan(zebraIndex);
    }

    @Test
    void shouldRemoveUser(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);
        executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "password");
        executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "bob",
                "-w", "password");

        // Remove alice
        var removeResult = executeCommand("--unlock-insecure-options", "remove-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice");

        assertThat(removeResult.exitCode()).isZero();
        assertThat(removeResult.stdout()).contains("User 'alice' removed successfully");

        // List should only show bob
        var listResult = executeCommand("--unlock-insecure-options", "list-users",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(listResult.exitCode()).isZero();
        assertThat(listResult.stdout())
                .contains("bob")
                .doesNotContain("alice");
    }

    @Test
    void shouldUpdatePassword(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);
        executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "old-password");

        // Update password
        var updateResult = executeCommand("--unlock-insecure-options", "update-password",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "new-password");

        assertThat(updateResult.exitCode()).isZero();
        assertThat(updateResult.stdout()).contains("Password for user 'alice' updated successfully");

        // User should still exist
        var listResult = executeCommand("--unlock-insecure-options", "list-users",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(listResult.exitCode()).isZero();
        assertThat(listResult.stdout()).contains("alice");
    }

    @Test
    void shouldFailWhenRemovingNonExistentUser(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);

        var removeResult = executeCommand("--unlock-insecure-options", "remove-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "nonexistent");

        assertThat(removeResult.exitCode()).isEqualTo(1);
        assertThat(removeResult.stderr())
                .contains("Failed to remove user")
                .contains("User 'nonexistent' not found");
    }

    @Test
    void shouldFailWhenUpdatingNonExistentUser(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);

        var updateResult = executeCommand("--unlock-insecure-options", "update-password",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "nonexistent",
                "-w", "password");

        assertThat(updateResult.exitCode()).isEqualTo(1);
        assertThat(updateResult.stderr())
                .contains("Failed to update password")
                .contains("User 'nonexistent' not found");
    }

    @Test
    void shouldFailWhenKeystoreFileNotFound(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("nonexistent.p12");

        var addResult = executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "password");

        assertThat(addResult.exitCode()).isEqualTo(1);
        assertThat(addResult.stderr())
                .contains("Failed to add user")
                .contains("KeyStore file not found");
    }

    @Test
    void shouldSupportScramSha256Mechanism(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);

        var addResult = executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "password",
                "-m", "SCRAM_SHA_256");

        assertThat(addResult.exitCode()).isZero();
        assertThat(addResult.stdout()).contains("User 'alice' added successfully");
    }

    @Test
    void shouldSupportScramSha512Mechanism(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        executeCommand("--unlock-insecure-options", "create", "-k", keystorePath.toString(), "-p", KEYSTORE_PASSWORD);

        var addResult = executeCommand("--unlock-insecure-options", "add-user",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD,
                "-u", "alice",
                "-w", "password",
                "-m", "SCRAM_SHA_512");

        assertThat(addResult.exitCode()).isZero();
        assertThat(addResult.stdout()).contains("User 'alice' added successfully");
    }

    @Test
    void shouldShowUsageWhenNoCommand() {
        var result = executeCommand();

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout())
                .contains("keystore-credential-tool")
                .contains("Manage SCRAM credentials")
                .contains("create")
                .contains("add-user")
                .contains("remove-user")
                .contains("update-password")
                .contains("list-users");
    }

    @Test
    void shouldBlockPasswordOptionsByDefault(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        // Try to create keystore with password option but without unlock flag
        var result = executeCommand("create",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(result.exitCode()).isEqualTo(2);
        assertThat(result.stderr())
                .contains("Password options are disabled by default for security")
                .contains("--unlock-insecure-options");
    }

    @Test
    void shouldWarnWhenUsingInsecureOptions(@TempDir Path tempDir) {
        Path keystorePath = tempDir.resolve("credentials.p12");

        // Create with unlock flag - should warn
        var result = executeCommand("--unlock-insecure-options", "create",
                "-k", keystorePath.toString(),
                "-p", KEYSTORE_PASSWORD);

        assertThat(result.exitCode()).isZero();
        assertThat(result.stderr())
                .contains("SECURITY WARNING")
                .contains("NOT RECOMMENDED")
                .contains("Process listings")
                .contains("Shell history");
    }

    /**
     * Execute the CLI tool with the given arguments.
     *
     * @param args command-line arguments
     * @return result containing exit code, stdout, and stderr
     */
    private CommandResult executeCommand(String... args) {
        var stdout = new StringWriter();
        var stderr = new StringWriter();

        var cmd = new CommandLine(new KeystoreCredentialTool())
                .setOut(new PrintWriter(stdout))
                .setErr(new PrintWriter(stderr));

        int exitCode = cmd.execute(args);

        return new CommandResult(exitCode, stdout.toString(), stderr.toString());
    }

    /**
     * Find the index of the first line containing the search term.
     */
    private int findLineIndex(java.util.List<String> lines, String searchTerm) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains(searchTerm)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Result of executing a CLI command.
     */
    private record CommandResult(int exitCode, String stdout, String stderr) {}
}
