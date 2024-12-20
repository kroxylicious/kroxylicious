/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.service.AbstractTestKmsFacadeTest;
import io.kroxylicious.kms.service.TestKekManager.AlreadyExistsException;
import io.kroxylicious.kms.service.UnknownAliasException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class FortanixDsmKmsTestKmsFacadeTest extends AbstractTestKmsFacadeTest<Config, String, FortanixDsmKmsEdek> {

    FortanixDsmKmsTestKmsFacadeTest() {
        super(new FortanixDsmKmsTestKmsFacadeFactory());
    }

    @BeforeEach
    void beforeEach() {
        assumeThat(FortanixDsmKmsTestKmsFacade.AVAILABLE).isTrue();
    }

    @Test
    void classAndConfig() {
        try (var facade = factory.build()) {
            facade.start();
            assertThat(facade.getKmsServiceClass()).isEqualTo(FortanixDsmKmsService.class);
            assertThat(facade.getKmsServiceConfig()).isInstanceOf(Config.class);
        }
    }

    @Test
    void generateKekFailsIfAliasExists() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            manager.generateKek(ALIAS);

            assertThatThrownBy(() -> manager.generateKek(ALIAS))
                    .isInstanceOf(AlreadyExistsException.class);
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
