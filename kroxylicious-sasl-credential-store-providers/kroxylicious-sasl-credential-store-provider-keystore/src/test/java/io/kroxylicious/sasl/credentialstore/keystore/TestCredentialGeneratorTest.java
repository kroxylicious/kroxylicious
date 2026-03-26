/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.SecretKey;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for TestCredentialGenerator.
 */
class TestCredentialGeneratorTest {

    private static final String KEYSTORE_PASSWORD = "test-password";

    @Test
    void shouldGenerateDeterministicCredentials() throws Exception {
        // Given - a SecureRandom that returns a known salt
        SecureRandom mockRandom = mock(SecureRandom.class);
        byte[] knownSalt = new byte[20];
        Arrays.fill(knownSalt, (byte) 42);

        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
            System.arraycopy(knownSalt, 0, bytes, 0, bytes.length);
            return null;
        }).when(mockRandom).nextBytes(any());

        var generator = new TestCredentialGenerator(mockRandom);

        // When - generate credential with known inputs
        String username = "alice";
        String password = "alice-secret";
        ScramCredential credential = generator.generateScramCredential(
                username,
                password,
                ScramMechanism.SCRAM_SHA_256);

        // Then - verify deterministic salt
        assertThat(credential.salt()).isEqualTo(knownSalt);
        assertThat(credential.username()).isEqualTo(username);
        assertThat(credential.iterations()).isEqualTo(4096);
        assertThat(credential.hashAlgorithm()).isEqualTo("SHA-256");

        // And - verify keys are computed correctly using Kafka's ScramFormatter
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
        byte[] expectedSaltedPassword = formatter.saltedPassword(password, knownSalt, 4096);
        byte[] expectedServerKey = formatter.serverKey(expectedSaltedPassword);
        byte[] expectedClientKey = formatter.clientKey(expectedSaltedPassword);
        byte[] expectedStoredKey = formatter.storedKey(expectedClientKey);

        assertThat(credential.serverKey()).isEqualTo(expectedServerKey);
        assertThat(credential.storedKey()).isEqualTo(expectedStoredKey);
    }

    @Test
    void shouldGenerateConsistentCredentialsForSameInput() {
        // Given - fixed salt via mock
        SecureRandom mockRandom = mock(SecureRandom.class);
        byte[] fixedSalt = new byte[20];
        Arrays.fill(fixedSalt, (byte) 123);

        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
            System.arraycopy(fixedSalt, 0, bytes, 0, bytes.length);
            return null;
        }).when(mockRandom).nextBytes(any());

        var generator = new TestCredentialGenerator(mockRandom);

        // When - generate same credential twice
        ScramCredential cred1 = generator.generateScramCredential(
                "bob",
                "bob-password",
                ScramMechanism.SCRAM_SHA_256);

        ScramCredential cred2 = generator.generateScramCredential(
                "bob",
                "bob-password",
                ScramMechanism.SCRAM_SHA_256);

        // Then - credentials should be identical
        assertThat(cred1).isEqualTo(cred2);
    }

    @Test
    void shouldGenerateDifferentCredentialsForDifferentPasswords() {
        // Given - fixed salt
        SecureRandom mockRandom = mock(SecureRandom.class);
        byte[] fixedSalt = new byte[20];
        Arrays.fill(fixedSalt, (byte) 99);

        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
            System.arraycopy(fixedSalt, 0, bytes, 0, bytes.length);
            return null;
        }).when(mockRandom).nextBytes(any());

        var generator = new TestCredentialGenerator(mockRandom);

        // When - generate credentials with different passwords
        ScramCredential cred1 = generator.generateScramCredential(
                "user",
                "password1",
                ScramMechanism.SCRAM_SHA_256);

        ScramCredential cred2 = generator.generateScramCredential(
                "user",
                "password2",
                ScramMechanism.SCRAM_SHA_256);

        // Then - credentials should differ
        assertThat(cred1.serverKey()).isNotEqualTo(cred2.serverKey());
        assertThat(cred1.storedKey()).isNotEqualTo(cred2.storedKey());
    }

    @Test
    void shouldRoundTripCredentialsThroughKeyStore(@TempDir Path tempDir) throws Exception {
        // Given - generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("test.jks");
        var generator = new TestCredentialGenerator();

        String username1 = "alice";
        String password1 = "alice-secret";
        String username2 = "bob";
        String password2 = "bob-secret";

        generator.generateKeyStore(
                keystorePath,
                KEYSTORE_PASSWORD,
                username1, password1,
                username2, password2);

        // When - load KeyStore and extract credentials
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (var fis = Files.newInputStream(keystorePath)) {
            keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }

        // Then - KeyStore should contain both users
        assertThat(keyStore.containsAlias(username1)).isTrue();
        assertThat(keyStore.containsAlias(username2)).isTrue();

        // And - extract and deserialize credentials
        ScramCredentialSerializer serializer = new ScramCredentialSerializer();

        KeyStore.SecretKeyEntry entry1 = (KeyStore.SecretKeyEntry) keyStore.getEntry(
                username1,
                new KeyStore.PasswordProtection(KEYSTORE_PASSWORD.toCharArray()));
        SecretKey secretKey1 = entry1.getSecretKey();
        ScramCredential credential1 = serializer.deserialize(secretKey1.getEncoded(), username1);

        KeyStore.SecretKeyEntry entry2 = (KeyStore.SecretKeyEntry) keyStore.getEntry(
                username2,
                new KeyStore.PasswordProtection(KEYSTORE_PASSWORD.toCharArray()));
        SecretKey secretKey2 = entry2.getSecretKey();
        ScramCredential credential2 = serializer.deserialize(secretKey2.getEncoded(), username2);

        // Then - credentials should have correct usernames
        assertThat(credential1.username()).isEqualTo(username1);
        assertThat(credential2.username()).isEqualTo(username2);

        // And - credentials should be valid (non-empty keys)
        assertThat(credential1.salt()).isNotEmpty();
        assertThat(credential1.serverKey()).isNotEmpty();
        assertThat(credential1.storedKey()).isNotEmpty();

        assertThat(credential2.salt()).isNotEmpty();
        assertThat(credential2.serverKey()).isNotEmpty();
        assertThat(credential2.storedKey()).isNotEmpty();
    }

    @Test
    void shouldGenerateValidScramSha512Credentials() {
        // Given
        var generator = new TestCredentialGenerator();

        // When - generate SHA-512 credential
        ScramCredential credential = generator.generateScramCredential(
                "user",
                "password",
                ScramMechanism.SCRAM_SHA_512);

        // Then
        assertThat(credential.hashAlgorithm()).isEqualTo("SHA-512");
        assertThat(credential.salt()).isNotEmpty();
        assertThat(credential.serverKey()).isNotEmpty();
        assertThat(credential.storedKey()).isNotEmpty();
        assertThat(credential.iterations()).isEqualTo(4096);
    }
}
