/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void detectsMissingInitialization() {
        assertThatThrownBy(() -> awsKmsService.buildKms())
                .isInstanceOf(NullPointerException.class);
    }
}
