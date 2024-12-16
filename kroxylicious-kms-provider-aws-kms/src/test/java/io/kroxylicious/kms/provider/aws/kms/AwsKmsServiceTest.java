/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;
import java.util.Optional;

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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsKmsServiceTest {

    @Mock
    private CredentialsProviderFactory factory;

    @Mock
    private LongTermCredentialsProviderConfig longTermCredentialsProviderConfig;

    @Mock
    private CredentialsProvider credentialsProvider;

    private AwsKmsService awsKmsService;

    @BeforeEach
    void beforeEach() {

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
        when(factory.createCredentialsProvider(isA(Config.class))).thenReturn(credentialsProvider);
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
        when(factory.createCredentialsProvider(isA(Config.class))).thenReturn(credentialsProvider);
        var config = new Config(URI.create("https://host.invalid"), null, null, longTermCredentialsProviderConfig, null, "us-east-1", null);
        awsKmsService.initialize(config);

        // When
        awsKmsService.close();

        // Then
        verify(credentialsProvider).close();
    }
}
