/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.List;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link KeystoreCredentialManager} CRUD operations.
 */
class KeystoreCredentialManagerTest {

    private static final String KEYSTORE_PASSWORD = "test-password";
    private static final String STORE_TYPE = "PKCS12";

    @Test
    void shouldCreateEmptyKeyStore(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();

        // When
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // Then
        KeyStore keyStore = KeyStore.getInstance(STORE_TYPE);
        try (var fis = java.nio.file.Files.newInputStream(keystorePath)) {
            keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }

        assertThat(keyStore.size()).isZero();
    }

    @Test
    void shouldAddUserToKeyStore(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // When
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "alice-secret", ScramMechanism.SCRAM_SHA_256);

        // Then
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice");
    }

    @Test
    void shouldAddMultipleUsers(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // When
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "alice-secret", ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "bob", "bob-secret", ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "charlie", "charlie-secret", ScramMechanism.SCRAM_SHA_512);

        // Then
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @Test
    void shouldRemoveUserFromKeyStore(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "alice-secret", ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "bob", "bob-secret", ScramMechanism.SCRAM_SHA_256);

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
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // When/Then
        assertThatThrownBy(() -> manager.removeUser(keystorePath, KEYSTORE_PASSWORD, "alice"))
                .isInstanceOf(KeyStoreException.class)
                .hasMessageContaining("User 'alice' not found");
    }

    @Test
    void shouldUpdateUserPassword(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "old-password", ScramMechanism.SCRAM_SHA_256);

        // When
        manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, "alice", "new-password", ScramMechanism.SCRAM_SHA_256);

        // Then - user still exists
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice");

        // And - credential was updated (different salt means different keys)
        KeyStore keyStore = KeyStore.getInstance(STORE_TYPE);
        try (var fis = java.nio.file.Files.newInputStream(keystorePath)) {
            keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }
        assertThat(keyStore.containsAlias("alice")).isTrue();
    }

    @Test
    void shouldThrowWhenUpdatingNonExistentUser(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // When/Then
        assertThatThrownBy(() -> manager.updatePassword(keystorePath, KEYSTORE_PASSWORD, "alice", "new-password", ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(KeyStoreException.class)
                .hasMessageContaining("User 'alice' not found");
    }

    @Test
    void shouldListUsersInSortedOrder(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // When - add users in non-alphabetical order
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "zebra", "password", ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "password", ScramMechanism.SCRAM_SHA_256);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "mike", "password", ScramMechanism.SCRAM_SHA_256);

        // Then - should be sorted alphabetically
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice", "mike", "zebra");
    }

    @Test
    void shouldReturnEmptyListWhenNoUsers(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);

        // When
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);

        // Then
        assertThat(users).isEmpty();
    }

    @Test
    void shouldReplaceUserWhenAddingExistingUser(@TempDir Path tempDir) throws Exception {
        // Given
        Path keystorePath = tempDir.resolve("test.p12");
        var manager = new KeystoreCredentialManager();
        manager.createKeyStore(keystorePath, KEYSTORE_PASSWORD, STORE_TYPE);
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "old-password", ScramMechanism.SCRAM_SHA_256);

        // When - add same user again with different password
        manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "new-password", ScramMechanism.SCRAM_SHA_256);

        // Then - still only one user
        List<String> users = manager.listUsers(keystorePath, KEYSTORE_PASSWORD);
        assertThat(users).containsExactly("alice");
    }

    @Test
    void shouldThrowWhenKeyStoreFileNotFound(@TempDir Path tempDir) {
        // Given
        Path keystorePath = tempDir.resolve("nonexistent.p12");
        var manager = new KeystoreCredentialManager();

        // When/Then
        assertThatThrownBy(() -> manager.addUser(keystorePath, KEYSTORE_PASSWORD, "alice", "password", ScramMechanism.SCRAM_SHA_256))
                .isInstanceOf(KeyStoreException.class)
                .hasMessageContaining("Failed to add user")
                .hasCauseInstanceOf(KeyStoreException.class)
                .cause()
                .hasMessageContaining("KeyStore file not found");
    }
}
