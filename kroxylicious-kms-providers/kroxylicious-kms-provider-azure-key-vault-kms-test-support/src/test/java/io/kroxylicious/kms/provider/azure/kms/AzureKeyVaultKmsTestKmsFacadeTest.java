/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

import org.junit.jupiter.api.Test;

import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;

import static org.assertj.core.api.Assertions.assertThat;

class AzureKeyVaultKmsTestKmsFacadeTest {

    @Test
    void lowKeyContainer() {
        try (LowkeyVaultContainer lowKeyContainer = AzureKeyVaultKmsTestKmsFacade.createLowKeyContainer()) {
            assertThat(lowKeyContainer.getDockerImageName()).startsWith("mirror.gcr.io/nagyesta/lowkey-vault:");
        }
    }

}