/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustKmsService;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.testing.kms.TestKekManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CipherTrustTestKmsFacadeTest {

    private CipherTrustTestKmsFacade facade;
    private TestKekManager manager;

    @BeforeEach
    void setUp() {
        facade = new CipherTrustTestKmsFacade();
        facade.start();
        manager = facade.getTestKekManager();
    }

    @AfterEach
    void tearDown() {
        if (facade != null) {
            facade.close();
        }
    }

    @Test
    void classAndConfig() {
        // When/Then
        assertThat(facade.getKmsServiceClass()).isEqualTo(CipherTrustKmsService.class);
        assertThat(facade.getKmsServiceConfig()).isInstanceOf(Config.class);
    }

    @Test
    void shouldGenerateValidConfig() {
        // When
        var config = facade.getKmsServiceConfig();

        // Then (default is CLIENT_CERT mode)
        assertThat(config).isNotNull();
        assertThat(config.endpointUrl()).isNotNull();
        assertThat(config.clientCredentials()).isNotNull();
        assertThat(config.clientCredentials().clientId()).isNotEmpty();
    }

    @Test
    void shouldProvideTestKekManager() {
        // When/Then
        assertThat(manager).isNotNull();
    }

    @Test
    void generateKek() {
        // Given
        String alias = "test-key";

        // When
        manager.generateKek(alias);

        // Then
        assertThat(manager.read(alias)).isNotNull();
    }

    @Test
    void rotateKek() {
        // Given
        String alias = "rotate-test-key";
        manager.generateKek(alias);

        // When/Then
        assertThatNoException().isThrownBy(() -> manager.rotateKek(alias));
    }

    @Test
    void deleteKek() {
        // Given
        String alias = "delete-test-key";
        manager.generateKek(alias);

        // When
        manager.deleteKek(alias);

        // Then
        assertThatThrownBy(() -> manager.read(alias))
                .isInstanceOf(UnknownAliasException.class);
    }

    @Test
    void deleteKekNotFound() {
        // Given
        String nonExistentAlias = "non-existent-key";

        // When/Then
        assertThatThrownBy(() -> manager.deleteKek(nonExistentAlias))
                .isInstanceOf(UnknownAliasException.class);
    }

    @Test
    void rotateKekWithSpacesInName() {
        // Given - Create a key with spaces in the name
        String aliasWithSpaces = "rotate test key";
        manager.generateKek(aliasWithSpaces);

        // When/Then - Should successfully rotate (URI-encoding the name in the URL)
        assertThatNoException().isThrownBy(() -> manager.rotateKek(aliasWithSpaces));
    }

    @Test
    void deleteKekWithSpacesInName() {
        // Given - Create a key with spaces in the name
        String aliasWithSpaces = "delete test key";
        manager.generateKek(aliasWithSpaces);

        // When/Then - Should successfully delete (URI-encoding the name in the query parameter)
        assertThatNoException().isThrownBy(() -> manager.deleteKek(aliasWithSpaces));
    }

    @Nested
    @DisabledIfEnvironmentVariable(named = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT", matches = ".+", disabledReason = "Auth mode tests only apply to mock server")
    class AuthenticationModes {

        @Test
        @SetEnvironmentVariable(key = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE", value = "PASSWORD")
        void shouldConfigurePasswordAuthentication() {
            // Given: facade configured for password auth
            CipherTrustTestKmsFacade passwordFacade = new CipherTrustTestKmsFacade();

            // When: facade started
            passwordFacade.start();

            try {
                Config config = passwordFacade.getKmsServiceConfig();

                // Then: config contains user credentials (not client credentials)
                assertThat(config.userCredentials()).isNotNull();
                assertThat(config.userCredentials().username()).isNotNull();
                assertThat(config.userCredentials().password()).isNotNull();
                assertThat(config.clientCredentials()).isNull();
            }
            finally {
                passwordFacade.close();
            }
        }

        @Test
        @SetEnvironmentVariable(key = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE", value = "CLIENT_CERT")
        void shouldConfigureClientCertificateAuthentication() {
            // Given: facade configured for client cert auth
            CipherTrustTestKmsFacade clientCertFacade = new CipherTrustTestKmsFacade();

            // When: facade started
            clientCertFacade.start();

            try {
                Config config = clientCertFacade.getKmsServiceConfig();

                // Then: config contains client credentials (not user credentials)
                assertThat(config.clientCredentials()).isNotNull();
                assertThat(config.clientCredentials().clientId()).isNotEmpty();
                assertThat(config.userCredentials()).isNull();

                // And: TLS config includes client certificate
                assertThat(config.tls()).isNotNull();
                assertThat(config.tls().key()).isNotNull();
            }
            finally {
                clientCertFacade.close();
            }
        }

        @Test
        void shouldDefaultToClientCertificateMode() {
            // Given: no AUTH_MODE env var set (using default from outer test class setup)
            Config config = facade.getKmsServiceConfig();

            // Then: defaults to client cert mode
            assertThat(config.clientCredentials()).isNotNull();
            assertThat(config.userCredentials()).isNull();
        }

        @Test
        @SetEnvironmentVariable(key = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE", value = "PASSWORD")
        void shouldAuthenticateWithPasswordMode() {
            // Given: facade with password auth
            CipherTrustTestKmsFacade passwordFacade = new CipherTrustTestKmsFacade();
            passwordFacade.start();

            try {
                TestKekManager passwordManager = passwordFacade.getTestKekManager();
                String alias = "password-auth-test";

                // When: KEK operation performed
                passwordManager.generateKek(alias);

                // Then: authentication succeeds and operation completes
                assertThat(passwordManager.read(alias)).isNotNull();

                passwordManager.deleteKek(alias);
            }
            finally {
                passwordFacade.close();
            }
        }

        @Test
        @SetEnvironmentVariable(key = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE", value = "CLIENT_CERT")
        void shouldAuthenticateWithClientCertMode() {
            // Given: facade with client cert auth
            CipherTrustTestKmsFacade clientCertFacade = new CipherTrustTestKmsFacade();
            clientCertFacade.start();

            try {
                TestKekManager clientCertManager = clientCertFacade.getTestKekManager();
                String alias = "client-cert-auth-test";

                // When: KEK operation performed
                clientCertManager.generateKek(alias);

                // Then: authentication succeeds and operation completes
                assertThat(clientCertManager.read(alias)).isNotNull();

                clientCertManager.deleteKek(alias);
            }
            finally {
                clientCertFacade.close();
            }
        }
    }
}
