/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.proxy.config.secret.FilePassword;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyStore;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.tls.CertificateGenerator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for HashiCorp Vault with TLS, checking that we can make
 * API calls with TLS enabled on the server.
 */
class VaultKmsTlsIT {

    private static final String VAULT_TOKEN = "token";
    private TestVault vaultContainer;

    private static final CertificateGenerator.Keys keys = CertificateGenerator.generate();

    @BeforeEach
    void beforeEach() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();
        vaultContainer = TestVault.startWithTls(keys);
    }

    @AfterEach
    void afterEach() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    interface KmsCreator {
        VaultKms createKms(URI endpoint);
    }

    static List<Arguments> tlsConfigurations() {
        return List.of(
                Arguments.of("pkcs12Tls", (KmsCreator) (uri) -> getTlsVaultKms(tlsForTrustStoreInlinePassword(keys.pkcs12ClientTruststore()), uri)),
                Arguments.of("pkcs12NoPasswordTlsService",
                        (KmsCreator) (uri) -> getTlsVaultKms(tlsForTrustStoreNoPassword(keys.pkcs12NoPasswordClientTruststore()), uri)),
                Arguments.of("filePasswordTls", (KmsCreator) (uri) -> getTlsVaultKms(tlsForTrustStoreFilePassword(keys.pkcs12ClientTruststore()), uri)),
                Arguments.of("jksTls", (KmsCreator) (uri) -> getTlsVaultKms(tlsForTrustStoreInlinePassword(keys.jksClientTruststore()), uri)),
                Arguments.of("defaultStoreTypeTls", (KmsCreator) (uri) -> getTlsVaultKms(defaultStoreTypeTls(keys.jksClientTruststore()), uri)),
                Arguments.of("tlsInsecure", (KmsCreator) (uri) -> getTlsVaultKms(insecureTls(), uri)),
                Arguments.of("pkcs12Tls", (KmsCreator) (uri) -> getTlsVaultKms(tlsForTrustStoreInlinePassword(keys.pkcs12ClientTruststore()), uri)));
    }

    @Test
    void tlsConnectionFailsWithoutClientTrust() {
        try (VaultKmsService vaultKmsService = new VaultKmsService()) {
            var tlsConfig = vaultConfig(null, vaultContainer.getEndpoint());
            vaultKmsService.initialize(tlsConfig);
            var keyName = "mykey";
            createKek(keyName);
            var kms = vaultKmsService.buildKms();
            var resolved = kms.resolveAlias(keyName);
            assertThat(resolved)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat().havingCause()
                    .isInstanceOf(SSLHandshakeException.class)
                    .withMessageContaining("unable to find valid certification path to requested target");
        }
    }

    @Test
    void testMutualTlsConnectionRejectedIfClientOffersNoCert() {
        CertificateGenerator.Keys clientKeys = CertificateGenerator.generate();
        TestVault testVault = TestVault.startWithClientAuthTls(keys, clientKeys);
        var keyName = "mykey";
        testVault.createKek(keyName);
        VaultKms service = getTlsVaultKms(tlsForTrustStoreFilePassword(keys.pkcs12ClientTruststore()), testVault.getEndpoint());
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat().havingCause()
                .isInstanceOf(SSLHandshakeException.class)
                .withMessageContaining("Received fatal alert: certificate_required");
        testVault.close();
    }

    @Test
    void testMutualTlsConnectionAcceptedIfClientOffersTrustedCert() {
        CertificateGenerator.Keys clientKeys = CertificateGenerator.generate();
        TestVault testVault = TestVault.startWithClientAuthTls(keys, clientKeys);
        var keyName = "mykey";
        testVault.createKek(keyName);
        CertificateGenerator.TrustStore trustStore = keys.pkcs12ClientTruststore();
        CertificateGenerator.KeyStore keyStore = clientKeys.jksServerKeystore();
        VaultKms service = getTlsVaultKms(
                new Tls(new KeyStore(keyStore.path().toString(), new InlinePassword(keyStore.storePassword()), new InlinePassword(keyStore.keyPassword()), "JKS"),
                        new TrustStore(trustStore.path().toString(), new FilePassword(trustStore.passwordFile().toString()), trustStore.type(), null)),
                testVault.getEndpoint());
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(keyName);
        testVault.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("tlsConfigurations")
    void testArbitraryKmsOperationSucceedsWithTls(String kms, KmsCreator creator) {
        VaultKms vaultKms = creator.createKms(vaultContainer.getEndpoint());
        var keyName = "mykey";
        createKek(keyName);
        var resolved = vaultKms.resolveAlias(keyName);
        assertThat(resolved)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(keyName);

    }

    private ReadKeyData createKek(String keyId) {
        return vaultContainer.createKek(keyId);
    }

    @NonNull
    private static Tls insecureTls() {
        return new Tls(null, new InsecureTls(true));
    }

    @NonNull
    private static Tls defaultStoreTypeTls(CertificateGenerator.TrustStore jksTrustStore) {
        return new Tls(null, new TrustStore(jksTrustStore.path().toString(), new InlinePassword(jksTrustStore.password()), null, null));
    }

    @NonNull
    private static VaultKms getTlsVaultKms(Tls tls, URI endpoint) {
        var vaultKmsService = new VaultKmsService();
        vaultKmsService.initialize(vaultConfig(tls, endpoint));
        return vaultKmsService.buildKms();
    }

    @NonNull
    private static Config vaultConfig(Tls tls, URI endpoint) {
        return new Config(endpoint, new InlinePassword(VAULT_TOKEN), tls);
    }

    @NonNull
    private static Tls tlsForTrustStoreInlinePassword(CertificateGenerator.TrustStore trustStore) {
        return new Tls(null, new TrustStore(trustStore.path().toString(), new InlinePassword(trustStore.password()), trustStore.type(), null));
    }

    @NonNull
    private static Tls tlsForTrustStoreFilePassword(CertificateGenerator.TrustStore trustStore) {
        return new Tls(null, new TrustStore(trustStore.path().toString(), new FilePassword(trustStore.passwordFile().toString()), trustStore.type(), null));
    }

    @NonNull
    private static Tls tlsForTrustStoreNoPassword(CertificateGenerator.TrustStore trustStore) {
        return new Tls(null, new TrustStore(trustStore.path().toString(), null, trustStore.type(), null));
    }

}
