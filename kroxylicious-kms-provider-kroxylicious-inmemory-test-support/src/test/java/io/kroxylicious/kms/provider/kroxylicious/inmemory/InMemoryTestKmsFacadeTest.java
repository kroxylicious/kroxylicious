/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.AbstractTestKmsFacadeTest;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTestKmsFacadeTest extends AbstractTestKmsFacadeTest<Config, UUID, InMemoryEdek> {

    InMemoryTestKmsFacadeTest() {
        super(new InMemoryTestKmsFacadeFactory());
    }

    @Test
    void classAndConfig() {
        try (var facade = factory.build()) {
            facade.start();
            assertThat(facade.getKmsServiceClass()).isEqualTo(IntegrationTestingKmsService.class);
            assertThat(facade.getKmsServiceConfig()).isInstanceOf(Config.class);
            assertThat(facade.getKms()).isInstanceOf(InMemoryKms.class);
        }
    }

}
