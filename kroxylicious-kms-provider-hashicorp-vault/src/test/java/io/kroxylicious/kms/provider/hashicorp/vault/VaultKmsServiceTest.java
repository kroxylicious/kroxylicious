/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLParameters;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultKmsServiceTest {
    private VaultKmsService vaultKmsService;

    @BeforeEach
    void beforeEach() {
        vaultKmsService = new VaultKmsService();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(vaultKmsService).ifPresent(VaultKmsService::close);
    }

    @Test
    void detectsMissingInitialization() {
        assertThatThrownBy(() -> vaultKmsService.buildKms())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void applesTlsConfiguration() {
        var validButUnusualCipherSuite = "TLS_EMPTY_RENEGOTIATION_INFO_SCSV"; // Valid suite, but not a true cipher
        vaultKmsService.initialize(
                new Config(URI.create("https://unused/v1/transit"), new InlinePassword("vaultToken"), new Tls(null, null, new AllowDeny<>(
                        List.of(validButUnusualCipherSuite), null), null)));
        var kms = vaultKmsService.buildKms();
        var client = kms.getHttpClient();
        assertThat(client)
                .extracting(HttpClient::sslParameters)
                .extracting(SSLParameters::getCipherSuites, InstanceOfAssertFactories.array(String[].class))
                .containsExactly(validButUnusualCipherSuite);
    }

}
