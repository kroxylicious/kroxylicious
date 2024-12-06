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

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.CredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AwsKmsServiceTest {
    private AwsKmsService awsKmsService;

    @BeforeEach
    void beforeEach() {
        awsKmsService = new AwsKmsService();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(awsKmsService).ifPresent(AwsKmsService::close);
    }

    @Test
    @SuppressWarnings("unchecked")
    void initializeCreatesCredentialsProvider() {
        // Given
        var cpc = mock(CredentialsProviderConfig.class);
        var cp = mock(CredentialsProvider.class);
        var config = new Config(URI.create("https://host.invalid"), cpc, "us-east-1", null);
        when(cpc.createCredentialsProvider()).thenReturn(cp);

        // When
        awsKmsService.initialize(config);

        // Then
        verify(cpc).createCredentialsProvider();
    }

    @Test
    void detectsMissingInitialization() {
        assertThatThrownBy(() -> awsKmsService.buildKms())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void credentialsProviderLifecycle() {
        // Given
        var cpc = mock(CredentialsProviderConfig.class);
        var cp = mock(CredentialsProvider.class);
        var config = new Config(URI.create("https://host.invalid"), cpc, "us-east-1", null);
        when(cpc.createCredentialsProvider()).thenReturn(cp);
        awsKmsService.initialize(config);

        // When
        awsKmsService.close();

        // Then
        verify(cp).close();
    }
}
