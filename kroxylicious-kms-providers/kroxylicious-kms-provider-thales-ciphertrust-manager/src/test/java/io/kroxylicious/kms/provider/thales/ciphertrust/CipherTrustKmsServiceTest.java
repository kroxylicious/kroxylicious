/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLParameters;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.UserCredentials;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CipherTrustKmsServiceTest {

    private CipherTrustKmsService service;

    @AfterEach
    void afterEach() {
        Optional.ofNullable(service).ifPresent(CipherTrustKmsService::close);
    }

    @Test
    void initializeWithUserCredentials() {
        // Given
        service = new CipherTrustKmsService();
        var config = new Config(
                URI.create("https://ctm.example.com"),
                new UserCredentials("user", new InlinePassword("pass")),
                null,
                null);
        service.initialize(config);

        // When
        var kms = service.buildKms();

        // Then
        assertThat(kms).isNotNull();
    }

    @Test
    void detectsMissingInitialization() {
        // Given
        service = new CipherTrustKmsService();
        // When/Then
        assertThatThrownBy(() -> service.buildKms())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void tokenServiceLifecycle() {
        // Given
        service = new CipherTrustKmsService();
        var config = new Config(
                URI.create("https://ctm.example.com"),
                new UserCredentials("user", new InlinePassword("pass")),
                null,
                null);
        service.initialize(config);

        // Build KMS to trigger token service creation
        service.buildKms();

        // When/Then - close should not throw
        assertThatNoException().isThrownBy(() -> service.close());
    }

    @Test
    void appliesTlsConfiguration() {
        // Given
        var validButUnusualCipherSuite = "TLS_EMPTY_RENEGOTIATION_INFO_SCSV";
        var config = new Config(
                URI.create("https://ctm.example.com"),
                new UserCredentials("user", new InlinePassword("pass")),
                null,
                new Tls(null, null, new AllowDeny<>(
                        List.of(validButUnusualCipherSuite), null), null, null));

        service = new CipherTrustKmsService();
        service.initialize(config);
        // When
        var kms = service.buildKms();

        // Then
        var client = ((CipherTrustKms) kms).getHttpClient();
        assertThat(client)
                .extracting(HttpClient::sslParameters)
                .extracting(SSLParameters::getCipherSuites, InstanceOfAssertFactories.array(String[].class))
                .containsExactly(validButUnusualCipherSuite);
    }

}
