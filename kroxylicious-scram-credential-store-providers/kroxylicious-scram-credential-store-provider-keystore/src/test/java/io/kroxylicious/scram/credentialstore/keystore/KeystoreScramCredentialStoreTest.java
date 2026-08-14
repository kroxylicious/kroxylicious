/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.concurrent.CompletionStage;

import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramHashAlgorithm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeystoreScramCredentialStoreTest {

    private static final String STORE_PASSWORD = "test-password";
    private static final String ALICE_PASSWORD = "alice-secret";
    private static final String BOB_PASSWORD = "bob-secret-pw";

    @TempDir
    Path tempDir;

    private Path keystorePath;
    private KeystoreScramCredentialStoreConfig config;
    private KeystoreScramCredentialStore store;

    @BeforeEach
    void setUp() throws Exception {
        keystorePath = tempDir.resolve("test-credentials.p12");

        // Generate test keystore with two users
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(keystorePath, STORE_PASSWORD, ScramMechanism.SCRAM_SHA_256, "alice", ALICE_PASSWORD, "bob", BOB_PASSWORD);

        if (Files.getFileAttributeView(keystorePath, PosixFileAttributeView.class) != null) {
            Files.setPosixFilePermissions(keystorePath, PosixFilePermissions.fromString("rw-------"));
        }

        config = new KeystoreScramCredentialStoreConfig(
                keystorePath.toString(),
                new InlinePassword(STORE_PASSWORD));

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
        assertThat(credential.hashAlgorithm()).isEqualTo(ScramHashAlgorithm.SHA_256);
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
                "/non/existent/keystore.p12",
                new InlinePassword(STORE_PASSWORD));

        assertThatThrownBy(() -> new KeystoreScramCredentialStore(badConfig))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("Failed to load KeyStore");
    }

    @Test
    void shouldThrowExceptionForInvalidPassword() {
        KeystoreScramCredentialStoreConfig badConfig = new KeystoreScramCredentialStoreConfig(
                keystorePath.toString(),
                new InlinePassword("wrong-password"));

        assertThatThrownBy(() -> new KeystoreScramCredentialStore(badConfig))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("Failed to load KeyStore");
    }

    @Test
    void shouldHandleEmptyKeyStore() throws Exception {
        Path emptyKeystorePath = tempDir.resolve("empty.p12");
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(emptyKeystorePath, STORE_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        if (Files.getFileAttributeView(emptyKeystorePath, PosixFileAttributeView.class) != null) {
            Files.setPosixFilePermissions(emptyKeystorePath, PosixFilePermissions.fromString("rw-------"));
        }

        KeystoreScramCredentialStoreConfig emptyConfig = new KeystoreScramCredentialStoreConfig(
                emptyKeystorePath.toString(),
                new InlinePassword(STORE_PASSWORD));

        KeystoreScramCredentialStore emptyStore = new KeystoreScramCredentialStore(emptyConfig);
        ScramCredential credential = emptyStore.lookupCredential("anyone").toCompletableFuture().join();

        assertThat(credential).isNull();
    }

    @Test
    void shouldAcceptOwnerOnlyPermissions() throws Exception {
        assumePosixPermissions();
        Files.setPosixFilePermissions(keystorePath, PosixFilePermissions.fromString("rw-------"));

        assertThatCode(() -> new KeystoreScramCredentialStore(config))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectWorldReadable() throws Exception {
        assertRejectsPermission("rw-r--r--");
    }

    @Test
    void shouldRejectWorldWritable() throws Exception {
        assertRejectsPermission("rw----rw-");
    }

    @Test
    void shouldRejectGroupReadable() throws Exception {
        assertRejectsPermission("rw-r-----");
    }

    @Test
    void shouldRejectGroupWritable() throws Exception {
        assertRejectsPermission("rw--w----");
    }

    @Test
    void shouldSkipTrustedCertificateEntries() throws Exception {
        // Given - a keystore that also has a trusted certificate entry (non-key entry)
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(keystorePath.toFile())) {
            ks.load(fis, STORE_PASSWORD.toCharArray());
        }
        ks.setCertificateEntry("trusted-ca", loadTestCertificate());
        try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
            ks.store(fos, STORE_PASSWORD.toCharArray());
        }
        KeystoreFilePermissions.setOwnerOnly(keystorePath);

        // When
        KeystoreScramCredentialStore loaded = new KeystoreScramCredentialStore(config);

        // Then - credential entries are still accessible; certificate entry was silently skipped
        assertThat(loaded.lookupCredential("alice").toCompletableFuture().join()).isNotNull();
        assertThat(loaded.lookupCredential("bob").toCompletableFuture().join()).isNotNull();
    }

    @Test
    void shouldSkipPrivateKeyEntries() throws Exception {
        // Given - a keystore that also has a private key entry (key entry, but not a SecretKeyEntry)
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(keystorePath.toFile())) {
            ks.load(fis, STORE_PASSWORD.toCharArray());
        }
        Certificate cert = loadTestCertificate();
        var kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(1024);
        var kp = kpg.generateKeyPair();
        ks.setEntry(
                "private-key-entry",
                new KeyStore.PrivateKeyEntry(kp.getPrivate(), new Certificate[]{ cert }),
                new KeyStore.PasswordProtection(STORE_PASSWORD.toCharArray()));
        try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
            ks.store(fos, STORE_PASSWORD.toCharArray());
        }
        KeystoreFilePermissions.setOwnerOnly(keystorePath);

        // When
        KeystoreScramCredentialStore loaded = new KeystoreScramCredentialStore(config);

        // Then - credential entries are still accessible; private key entry was silently skipped
        assertThat(loaded.lookupCredential("alice").toCompletableFuture().join()).isNotNull();
        assertThat(loaded.lookupCredential("bob").toCompletableFuture().join()).isNotNull();
    }

    private void assertRejectsPermission(String permString) throws Exception {
        assumePosixPermissions();
        Files.setPosixFilePermissions(keystorePath, PosixFilePermissions.fromString(permString));

        assertThatThrownBy(() -> new KeystoreScramCredentialStore(config))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("insecure permissions");
    }

    private void assumePosixPermissions() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                Files.getFileAttributeView(keystorePath, PosixFileAttributeView.class) != null,
                "POSIX file permissions not supported on this platform");
    }

    // Self-signed test certificate (CN=localhost), used only to populate non-credential keystore entries.
    private static final String TEST_CERT_PEM = """
            -----BEGIN CERTIFICATE-----
            MIICzDCCAbSgAwIBAgIJAPnSacXpIgPLMA0GCSqGSIb3DQEBDAUAMBQxEjAQBgNV
            BAMTCWxvY2FsaG9zdDAeFw0yMzA2MjIxNDAyNDBaFw0zMzA0MzAxNDAyNDBaMBQx
            EjAQBgNVBAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
            ggEBAJrZWdUMnO7X6v7yyBprtmISUTmnMuuFU2G83FQXSRSZiF18xB6nHyF7cZoP
            uBBBsJkBF+NHyGnK3QHsP1ZAgsd3IUxKZxChDY4ELHQEnJ08y0E8vMO5D8X2EIlP
            YwN9T+ReZfPp39SGuNc4pmpO29kGDxtIaEjVsCM6Sy32vayWn+7j2QkJvMlIt52w
            ev3aFNSfnI6lUMopgji5HibW0Wg+tUdbzQVTHpbsZjUSx07WUyZiKh9bCj78jOCU
            xeyvce7wKmW8OryWHSV1L96ATVCaa7j8dMl2WVcL5NdsrbzG48c4qoNGQlNuXP3k
            1Fak+B8HQuAy64KtgU7puRiIsE8CAwEAAaMhMB8wHQYDVR0OBBYEFGK4jT23mUpt
            SDF83zfem+JglvIRMA0GCSqGSIb3DQEBDAUAA4IBAQAlcTRvYNVOH4LCU9K42GEQ
            kTtli4uTeHNEDUX/GVnGqxIjofPCg2pTBLPkYQmKCnElaHqdKJOG8snw2NiFD0sN
            K1T+JPHvAnAVN0OVFBKZMTqK4sDm9bYzN1hCUKc4cWj9l/YXQ6uHFXSDxhek+qvI
            4yF/fSDBuYhf4w0Uyr4rmpC0dV+hdPFooFkdqprlkyI7ntCbqzzXMDuqKW7UyTN8
            GiY5W+u1+slIINm5o2UwzC2FpBRkWDwd/hxMHdcL6txYUDQF+Xy9xRKv9JIeOaXg
            0M91ZTBSxfxr2cn41AulsTENc6vKvz8Zhd8XSZWHlsGVRTQSAV3ibz+KF+mAYeRG
            -----END CERTIFICATE-----
            """;

    private static Certificate loadTestCertificate() throws Exception {
        return CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(TEST_CERT_PEM.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void shouldFailIfEntryIsMalformed() throws Exception {
        // Given - a keystore with a SecretKey entry containing garbage (not valid JSON)
        Path malformedKeystorePath = tempDir.resolve("malformed.p12");
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, STORE_PASSWORD.toCharArray());
        byte[] garbage = new byte[]{ 1, 2, 3, 4, 5 };
        ks.setEntry(
                KeystoreCredentialManager.hashUsername("alice"),
                new KeyStore.SecretKeyEntry(new SecretKeySpec(garbage, "AES")),
                new KeyStore.PasswordProtection(STORE_PASSWORD.toCharArray()));
        try (FileOutputStream fos = new FileOutputStream(malformedKeystorePath.toFile())) {
            ks.store(fos, STORE_PASSWORD.toCharArray());
        }
        if (Files.getFileAttributeView(malformedKeystorePath, PosixFileAttributeView.class) != null) {
            Files.setPosixFilePermissions(malformedKeystorePath, PosixFilePermissions.fromString("rw-------"));
        }
        KeystoreScramCredentialStoreConfig malformedConfig = new KeystoreScramCredentialStoreConfig(
                malformedKeystorePath.toString(),
                new InlinePassword(STORE_PASSWORD));

        // When/Then
        assertThatThrownBy(() -> new KeystoreScramCredentialStore(malformedConfig))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("Malformed credential");
    }
}
