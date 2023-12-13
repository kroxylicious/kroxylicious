/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.TestKekManager.AlreadyExistsException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
public abstract class AbstractTestKmsFacadeTest<C, K, E> {

    private static final String ALIAS = "myalias";
    private final Duration timeout;
    private final TestKmsFacadeFactory<C, K, E> factory;

    public AbstractTestKmsFacadeTest(TestKmsFacadeFactory<C, K, E> factory, Duration timeout) {
        this.factory = factory;
        this.timeout = timeout;
    }

    @Test
    void factory() {
        try (var facade = factory.build()) {
            assertThat(facade)
                    .isNotNull()
                    .extracting(TestKmsFacade::isAvailable)
                    .isEqualTo(true);
        }
    }

    @Test
    void generateKek() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            var generate = manager.generateKek(ALIAS);
            assertThat(generate).succeedsWithin(timeout);
        }
    }

    @Test
    void generateKekFailsIfAliasExists() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            var generate = manager.generateKek(ALIAS);
            assertThat(generate).succeedsWithin(timeout);

            assertThat(manager.generateKek(ALIAS))
                    .failsWithin(timeout)
                    .withThrowableThat()
                    .withCauseInstanceOf(AlreadyExistsException.class);
        }
    }

    @Test
    void rotateKek() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            var generate = manager.generateKek(ALIAS);
            assertThat(generate).succeedsWithin(timeout);

            var rotate = manager.rotateKek(ALIAS);
            assertThat(rotate).succeedsWithin(timeout);
        }
    }

    @Test
    void rotateKekFailsIfAliasDoesNotExist() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();

            assertThat(manager.rotateKek(ALIAS))
                    .failsWithin(timeout)
                    .withThrowableThat()
                    .withCauseInstanceOf(UnknownAliasException.class);
        }
    }
}
