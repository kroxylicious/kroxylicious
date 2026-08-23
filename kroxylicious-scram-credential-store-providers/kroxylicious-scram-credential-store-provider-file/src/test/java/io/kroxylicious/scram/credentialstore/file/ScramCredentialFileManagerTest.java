/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ScramCredentialFileManager} CRUD operations.
 */
class ScramCredentialFileManagerTest {

    private static final String KEYSTORE_PASSWORD = "test-keystore-password-123";
    private static final String USER_PASSWORD = "user-password-secret-456";

    @Test
    void shouldCreateEmptyKeyStore(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();

        // When
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // Then
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (var fis = java.nio.file.Files.newInputStream(keystorePath)) {
            keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }

        assertThat(manager.listUsers(keystorePath, KEYSTORE_PASSWORD)).isEmpty();
        assertThat(keyStore.containsAlias(ScramCredentialFileManager.PHANTOM_SALT_KEY_ALIAS)).isTrue();
    }

    @Test
    void shouldAddUserToKeyStore(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        // Then
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice");
    }

    @Test
    void shouldAddMultipleUsers(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "bob", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "charlie", USER_PASSWORD, ScramMechanism.SCRAM_SHA_512);

        // Then
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @Test
    void shouldRemoveUserFromKeyStore(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "bob", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        // When
        manager.removeUser(keystorePath, KEYSTORE_PASSWORD, "alice");

        // Then
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("bob");
    }

    @Test
    void shouldThrowWhenRemovingNonExistentUser(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.removeUser(keystorePath, KEYSTORE_PASSWORD, "alice"))
                .isInstanceOf(ScramCredentialFileException.class)
                .hasMessageContaining("User 'alice' not found");
    }

    @Test
    void shouldUpdateUserPassword(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "old-password-123", ScramMechanism.SCRAM_SHA_256);

        // When
        manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, "alice", "new-password-456", ScramMechanism.SCRAM_SHA_256);

        // Then - user still exists
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice");

        // And - credential was updated (different salt means different keys)
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (var fis = java.nio.file.Files.newInputStream(keystorePath)) {
            keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }
        assertThat(keyStore.containsAlias(ScramCredentialFileManager.hashUsername("alice"))).isTrue();
    }

    @Test
    void shouldThrowWhenUpdatingNonExistentUser(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, "alice", "new-password-456", ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(ScramCredentialFileException.class)
                .hasMessageContaining("User 'alice' not found");
    }

    @Test
    void shouldListUsersInSortedOrder(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When - add users in non-alphabetical order
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "zebra", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "mike", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        // Then - should be sorted alphabetically
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice", "mike", "zebra");
    }

    @Test
    void shouldReturnEmptyListWhenNoUsers(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);

        // Then
        assertThat(users).isEmpty();
    }

    @Test
    void shouldReplaceUserWhenAddingExistingUser(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "old-password-123", ScramMechanism.SCRAM_SHA_256);

        // When - add same user again with different password
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "new-password-456", ScramMechanism.SCRAM_SHA_256);

        // Then - still only one user
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice");
    }

    @Test
    void shouldRejectCreateWhenFileAlreadyExists(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD))
                .isInstanceOf(ScramCredentialFileException.class)
                .hasMessageContaining("SCRAM credential file already exists");
    }

    @Test
    void shouldThrowWhenKeyStoreFileNotFound(@TempDir Path tempDir) {
        // Given
        Path keystorePath = tempDir.resolve("nonexistent.p12");
        var manager = new ScramCredentialFileManager();

        // When/Then
        assertThatThrownBy(() -> manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(ScramCredentialFileException.class)
                .hasMessageContaining("SCRAM credential file not found");
    }

    // Password validation tests

    @Test
    void shouldRejectShortKeystorePasswordOnCreate(@TempDir Path tempDir) {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();

        // When/Then
        assertThatThrownBy(() -> manager.createKeyStore(keystorePath, "short"))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("File password must be at least 12 characters long")
                .hasMessageContaining("NIST recommends");
    }

    @Test
    void shouldRejectShortUserPassword(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "short", ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("User password must be at least 12 characters long")
                .hasMessageContaining("NIST recommends");
    }

    @Test
    void shouldRejectShortPasswordOnUpdate(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        // When/Then
        assertThatThrownBy(() -> manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, "alice", "short", ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("New password must be at least 12 characters long");
    }

    @Test
    void shouldAcceptMinimumLengthPassword(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        String minimumPassword = "twelve-chars"; // exactly 12 characters

        // When
        manager.createKeyStore(keystorePath, minimumPassword);
        manager.addUser(keystorePath, minimumPassword, "alice", minimumPassword, ScramMechanism.SCRAM_SHA_256);

        // Then
        List<String> users = manager.listUsers(keystorePath, minimumPassword);
        assertThat(users).containsExactly("alice");
    }

    @Test
    void shouldAcceptLongPassword(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        String longPassword = "this-is-a-very-long-password-passphrase-for-maximum-security-123";

        // When
        manager.createKeyStore(keystorePath, longPassword);
        manager.addUser(keystorePath, longPassword, "alice", longPassword, ScramMechanism.SCRAM_SHA_256);

        // Then
        List<String> users = manager.listUsers(keystorePath, longPassword);
        assertThat(users).containsExactly("alice");
    }

    // Username validation tests

    @Test
    void shouldRejectNullUsername(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.addUser(keystorePath, KEYSTORE_PASSWORD, null, USER_PASSWORD, ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void shouldRejectEmptyUsername(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.addUser(keystorePath, KEYSTORE_PASSWORD, "", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void shouldRejectOversizedUsername(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        String longUsername = "a".repeat(256);

        // When/Then
        assertThatThrownBy(() -> manager.addUser(keystorePath, KEYSTORE_PASSWORD, longUsername, USER_PASSWORD, ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("must not exceed 255 characters");
    }

    @Test
    void shouldAcceptMaxLengthUsername(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        String maxUsername = "a".repeat(255);

        // When
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, maxUsername, USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        // Then
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly(maxUsername);
    }

    @Test
    void shouldRejectNullUsernameOnUpdate(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, null, USER_PASSWORD, ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void shouldRejectEmptyUsernameOnUpdate(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When/Then
        assertThatThrownBy(() -> manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, "", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void shouldAcceptPasswordWithoutSpecialCharacters(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        String passphrasePassword = "coffee sunrise laptop"; // 21 chars, no special characters

        // When - this should work (no composition rules)
        manager.createKeyStore(keystorePath, passphrasePassword);
        manager.addUser(keystorePath, passphrasePassword, "alice", passphrasePassword, ScramMechanism.SCRAM_SHA_256);

        // Then
        List<String> users = manager.listUsers(keystorePath, passphrasePassword);
        assertThat(users).containsExactly("alice");
    }

    // listCredentials tests

    @Test
    void shouldListCredentialsWithMechanismAndIterations(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "bob", USER_PASSWORD, ScramMechanism.SCRAM_SHA_512, 8192);

        // When
        List<ScramCredentialFileManager.UserCredentialInfo> credentials = manager.listCredentials(keystorePath, KEYSTORE_PASSWORD);

        // Then
        assertThat(credentials)
                .hasSize(2)
                .extracting(ScramCredentialFileManager.UserCredentialInfo::username)
                .containsExactly("alice", "bob");
        assertThat(credentials)
                .anySatisfy(c -> {
                    assertThat(c.username()).isEqualTo("alice");
                    assertThat(c.mechanism()).isEqualTo("SCRAM-SHA-256");
                    assertThat(c.iterations()).isEqualTo(10000);
                })
                .anySatisfy(c -> {
                    assertThat(c.username()).isEqualTo("bob");
                    assertThat(c.mechanism()).isEqualTo("SCRAM-SHA-512");
                    assertThat(c.iterations()).isEqualTo(8192);
                });
    }

    @Test
    void shouldReturnEmptyCredentialsWhenNoUsers(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new ScramCredentialFileManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);

        // When
        List<ScramCredentialFileManager.UserCredentialInfo> credentials = manager.listCredentials(keystorePath, KEYSTORE_PASSWORD);

        // Then
        assertThat(credentials).isEmpty();
    }

    // TestCredentialGenerator validation tests

    @Test
    void shouldRejectOddNumberOfUsersInGenerateKeyStore(@TempDir Path tempDir) {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var generator = new TestCredentialGenerator();

        // When/Then
        assertThatThrownBy(() -> generator.generateKeyStore(keystorePath, KEYSTORE_PASSWORD, ScramMechanism.SCRAM_SHA_256, "alice", USER_PASSWORD, "bob"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alternating username/password pairs");
    }

    // generateScramCredential validation tests

    @Test
    void shouldRejectIterationsBelowMinimum() {
        // Given
        var manager = new ScramCredentialFileManager();

        // When/Then
        assertThatThrownBy(() -> manager.generateScramCredential("alice", USER_PASSWORD, ScramMechanism.SCRAM_SHA_256, 100))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessageContaining("Iteration count must be at least");
    }
}
