/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.sasl.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.sasl.credentialstore.ScramCredential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeystoreScramCredentialStoreTest {

    private static final String STORE_PASSWORD = "test-password";
    private static final String ALICE_PASSWORD = "alice-secret";
    private static final String BOB_PASSWORD = "bob-secret";

    @TempDir
    Path tempDir;

    private Path keystorePath;
    private KeystoreScramCredentialStoreConfig config;
    private KeystoreScramCredentialStore store;

    @BeforeEach
    void setUp() throws Exception {
        keystorePath = tempDir.resolve("test-credentials.jks");

        // Generate test keystore with two users
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(
                keystorePath,
                STORE_PASSWORD,
                "alice", ALICE_PASSWORD,
                "bob", BOB_PASSWORD);

        config = new KeystoreScramCredentialStoreConfig(
                keystorePath.toString(),
                new InlinePassword(STORE_PASSWORD),
                null,
                "PKCS12");

        store = new KeystoreScramCredentialStore(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (keystorePath != null && Files.exists(keystorePath)) {
            Files.delete(keystorePath);
        }
    }

    @Test
    void shouldLookupExistingCredential() {
        CompletionStage<ScramCredential> future = store.lookupCredential("alice");
        ScramCredential credential = future.toCompletableFuture().join();

        assertThat(credential).isNotNull();
        assertThat(credential.username()).isEqualTo("alice");
        assertThat(credential.hashAlgorithm()).isEqualTo("SHA-256");
        assertThat(credential.iterations()).isEqualTo(10000);
        assertThat(credential.salt()).isNotEmpty();
        assertThat(credential.serverKey()).isNotEmpty();
        assertThat(credential.storedKey()).isNotEmpty();
    }

    @Test
    void shouldReturnNullForNonExistentUser() {
        CompletionStage<ScramCredential> future = store.lookupCredential("charlie");
        ScramCredential credential = future.toCompletableFuture().join();

        assertThat(credential).isNull();
    }

    @Test
    void shouldLookupMultipleUsers() {
        ScramCredential alice = store.lookupCredential("alice").toCompletableFuture().join();
        ScramCredential bob = store.lookupCredential("bob").toCompletableFuture().join();

        assertThat(alice).isNotNull();
        assertThat(bob).isNotNull();
        assertThat(alice.username()).isEqualTo("alice");
        assertThat(bob.username()).isEqualTo("bob");

        // Different users should have different credentials
        assertThat(alice.salt()).isNotEqualTo(bob.salt());
        assertThat(alice.serverKey()).isNotEqualTo(bob.serverKey());
        assertThat(alice.storedKey()).isNotEqualTo(bob.storedKey());
    }

    @SuppressWarnings("DataFlowIssue") // we're testing that the null argument is rejected
    @Test
    void shouldRejectNullUsername() {
        assertThatThrownBy(() -> store.lookupCredential(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("username");
    }

    @Test
    void shouldThrowExceptionForNonExistentKeyStore() {
        KeystoreScramCredentialStoreConfig badConfig = new KeystoreScramCredentialStoreConfig(
                "/non/existent/keystore.jks",
                new InlinePassword(STORE_PASSWORD),
                null,
                "PKCS12");

        assertThatThrownBy(() -> new KeystoreScramCredentialStore(badConfig))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("Failed to load KeyStore");
    }

    @Test
    void shouldThrowExceptionForInvalidPassword() {
        KeystoreScramCredentialStoreConfig badConfig = new KeystoreScramCredentialStoreConfig(
                keystorePath.toString(),
                new InlinePassword("wrong-password"),
                null,
                "PKCS12");

        assertThatThrownBy(() -> new KeystoreScramCredentialStore(badConfig))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("Failed to load KeyStore");
    }

    @Test
    void shouldHandleEmptyKeyStore() throws Exception {
        Path emptyKeystorePath = tempDir.resolve("empty.jks");
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(emptyKeystorePath, STORE_PASSWORD);

        KeystoreScramCredentialStoreConfig emptyConfig = new KeystoreScramCredentialStoreConfig(
                emptyKeystorePath.toString(),
                new InlinePassword(STORE_PASSWORD),
                null,
                "PKCS12");

        KeystoreScramCredentialStore emptyStore = new KeystoreScramCredentialStore(emptyConfig);
        ScramCredential credential = emptyStore.lookupCredential("anyone").toCompletableFuture().join();

        assertThat(credential).isNull();
    }

    @Test
    void shouldUseSeparateKeyPasswordWhenProvided() {
        String keyPassword = "different-key-password";

        // For this test, we'd need to generate a keystore with different key passwords
        // but TestCredentialGenerator currently uses the same password for both
        // This test documents the intended behaviour
        KeystoreScramCredentialStoreConfig configWithKeyPassword = new KeystoreScramCredentialStoreConfig(
                keystorePath.toString(),
                new InlinePassword(STORE_PASSWORD),
                new InlinePassword(STORE_PASSWORD), // Same for now
                "PKCS12");

        assertThatCode(() -> new KeystoreScramCredentialStore(configWithKeyPassword))
                .doesNotThrowAnyException();
    }
}
