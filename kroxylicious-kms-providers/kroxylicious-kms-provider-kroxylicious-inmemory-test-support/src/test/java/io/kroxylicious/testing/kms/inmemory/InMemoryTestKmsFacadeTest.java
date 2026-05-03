/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.inmemory;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.testing.kms.AbstractTestKmsFacadeTest;
import io.kroxylicious.testing.kms.TestKekManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void generateKekFailsIfAliasExists() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            manager.generateKek(ALIAS);

            assertThatThrownBy(() -> manager.generateKek(ALIAS))
                    .isInstanceOf(TestKekManager.AlreadyExistsException.class);
        }
    }

    @Test
    void rotateKekFailsIfAliasDoesNotExist() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();

            assertThatThrownBy(() -> manager.rotateKek(ALIAS))
                    .isInstanceOf(UnknownAliasException.class);
        }
    }

    @Test
    void deleteKekFailsIfAliasDoesNotExist() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();

            assertThatThrownBy(() -> manager.deleteKek(ALIAS))
                    .isInstanceOf(UnknownAliasException.class);
        }
    }
}
