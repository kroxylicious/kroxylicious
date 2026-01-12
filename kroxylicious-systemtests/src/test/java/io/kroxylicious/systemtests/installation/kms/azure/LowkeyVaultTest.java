/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.azure;

import org.junit.jupiter.api.Test;

import static io.kroxylicious.systemtests.installation.kms.azure.LowkeyVault.LOWKEY_VAULT_IMAGE;
import static org.assertj.core.api.Assertions.assertThat;

class LowkeyVaultTest {

    // tactical test to prevent sonar failing the build for coverage reasons when renovate updates the image
    @Test
    void vaultImage() {
        assertThat(LOWKEY_VAULT_IMAGE).startsWith("mirror.gcr.io/nagyesta/lowkey-vault:");
    }

}