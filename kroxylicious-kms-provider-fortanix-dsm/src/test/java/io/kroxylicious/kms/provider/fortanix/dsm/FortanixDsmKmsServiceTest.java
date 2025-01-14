/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FortanixDsmKmsServiceTest {
    private FortanixDsmKmsService fortanixDsmKmsService;

    @BeforeEach
    void beforeEach() {
        fortanixDsmKmsService = new FortanixDsmKmsService();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(fortanixDsmKmsService).ifPresent(FortanixDsmKmsService::close);
    }

    @Test
    void detectsMissingInitialization() {
        assertThatThrownBy(() -> fortanixDsmKmsService.buildKms())
                .isInstanceOf(NullPointerException.class);
    }
}
