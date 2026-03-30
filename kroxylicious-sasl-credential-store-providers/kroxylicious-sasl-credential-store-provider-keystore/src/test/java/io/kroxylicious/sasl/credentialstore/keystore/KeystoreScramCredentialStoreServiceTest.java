/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeystoreScramCredentialStoreServiceTest {

    private static final String STORE_PASSWORD = "test-password";

    @TempDir
    Path tempDir;

    private Path keystorePath;
    private KeystoreScramCredentialStoreConfig config;
    private KeystoreScramCredentialStoreService service;

    @BeforeEach
    void setUp() throws Exception {
        keystorePath = tempDir.resolve("test-credentials.jks");

        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(
                keystorePath,
                STORE_PASSWORD,
                "alice", "alice-secret");

        config = new KeystoreScramCredentialStoreConfig(
                keystorePath.toString(),
                new InlinePassword(STORE_PASSWORD),
                null,
                "PKCS12");

        service = new KeystoreScramCredentialStoreService();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (service != null) {
            service.close();
        }
        if (keystorePath != null && Files.exists(keystorePath)) {
            Files.delete(keystorePath);
        }
    }

    @Test
    void shouldInitializeSuccessfully() {
        assertThatCode(() -> service.initialize(config))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldBuildCredentialStoreAfterInitialize() {
        service.initialize(config);
        ScramCredentialStore store = service.buildCredentialStore();

        assertThat(store).isNotNull()
                .isInstanceOf(KeystoreScramCredentialStore.class);
    }

    @Test
    void shouldBuildMultipleStores() {
        service.initialize(config);

        ScramCredentialStore store1 = service.buildCredentialStore();
        ScramCredentialStore store2 = service.buildCredentialStore();

        assertThat(store1).isNotNull();
        assertThat(store2).isNotNull();
        assertThat(store1).isNotSameAs(store2);
    }

    @Test
    void shouldRejectBuildBeforeInitialize() {
        assertThatThrownBy(() -> service.buildCredentialStore())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not been initialized");
    }

    @Test
    void shouldRejectDoubleInitialize() {
        service.initialize(config);

        assertThatThrownBy(() -> service.initialize(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already been initialized");
    }

    @Test
    void shouldRejectBuildAfterClose() {
        service.initialize(config);
        service.close();

        assertThatThrownBy(() -> service.buildCredentialStore())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("been closed");
    }

    @Test
    void shouldRejectInitializeAfterClose() {
        service.close();

        assertThatThrownBy(() -> service.initialize(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("been closed");
    }

    @Test
    void shouldAllowMultipleClose() {
        service.initialize(config);

        assertThatCode(() -> {
            service.close();
            service.close();
            service.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void shouldAllowCloseWithoutInitialize() {
        try (KeystoreScramCredentialStoreService uninitializedService = new KeystoreScramCredentialStoreService()) {

            assertThatCode(uninitializedService::close)
                    .doesNotThrowAnyException();
        }
    }
}
