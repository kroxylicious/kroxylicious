/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
