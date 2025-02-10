/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLParameters;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProviderFactory;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsKmsServiceTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    private CredentialsProviderFactory factory;

    @Mock
    private LongTermCredentialsProviderConfig longTermCredentialsProviderConfig;

    @Mock
    private CredentialsProvider credentialsProvider;

    private AwsKmsService awsKmsService;

    @BeforeEach
    void beforeEach() {
        when(factory.createCredentialsProvider(isA(Config.class))).thenReturn(credentialsProvider);
        awsKmsService = new AwsKmsService(factory);
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(awsKmsService).ifPresent(AwsKmsService::close);
    }

    @Test
    @SuppressWarnings("resource")
    void initializeCreatesCredentialsProvider() {
        // Given
        var config = new Config(URI.create("https://host.invalid"), null, null, longTermCredentialsProviderConfig, null, "us-east-1", null);

        // When
        awsKmsService.initialize(config);

        // Then
        verify(factory).createCredentialsProvider(config);
    }

    @Test
    void detectsMissingInitialization() {
        assertThatThrownBy(() -> awsKmsService.buildKms())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void credentialsProviderLifecycle() {
        // Given
        var config = new Config(URI.create("https://host.invalid"), null, null, longTermCredentialsProviderConfig, null, "us-east-1", null);
        awsKmsService.initialize(config);

        // When
        awsKmsService.close();

        // Then
        verify(credentialsProvider).close();
    }

    @Test
    void applesTlsConfiguration() {
        var validButUnusualCipherSuite = "TLS_EMPTY_RENEGOTIATION_INFO_SCSV"; // Valid suite, but not a true cipher
        var config = new Config(URI.create("https://host.invalid"), null, null,
                longTermCredentialsProviderConfig, null, "us-east1", new Tls(null, null, new AllowDeny<>(
                        List.of(validButUnusualCipherSuite), null), null));
        awsKmsService.initialize(
                config);
        var kms = awsKmsService.buildKms();
        var client = kms.getHttpClient();
        assertThat(client)
                .extracting(HttpClient::sslParameters)
                .extracting(SSLParameters::getCipherSuites, InstanceOfAssertFactories.array(String[].class))
                .containsExactly(validButUnusualCipherSuite);
    }

}
