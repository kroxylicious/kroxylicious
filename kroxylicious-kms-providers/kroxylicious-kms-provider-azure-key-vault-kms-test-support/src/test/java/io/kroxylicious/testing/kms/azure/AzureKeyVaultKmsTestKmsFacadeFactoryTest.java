/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.azure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AzureKeyVaultKmsTestKmsFacadeFactoryTest {

    @Test
    void buildReturnsFacade() {
        var factory = new AzureKeyVaultKmsTestKmsFacadeFactory();

        var facade = factory.build();

        assertThat(facade).isInstanceOf(AzureKeyVaultKmsTestKmsFacade.class);
    }
}
