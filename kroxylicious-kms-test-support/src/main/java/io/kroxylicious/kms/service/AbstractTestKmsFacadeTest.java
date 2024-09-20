/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.Objects;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@SuppressWarnings("java:S5960") // this is test code, it exists in the main module to facilitate its use by concrete test cases
public abstract class AbstractTestKmsFacadeTest<C, K, E> {

    protected static final String ALIAS = "myalias";
    protected final TestKmsFacadeFactory<C, K, E> factory;

    protected AbstractTestKmsFacadeTest(TestKmsFacadeFactory<C, K, E> factory) {
        Objects.requireNonNull(factory);
        this.factory = factory;
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
            assertThat(manager.exists(ALIAS)).isFalse();
            manager.generateKek(ALIAS);
            assertThat(manager.exists(ALIAS)).isTrue();
        }
    }


    @Test
    void rotateKek() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            manager.generateKek(ALIAS);
            assertThat(manager.exists(ALIAS)).isTrue();

            manager.rotateKek(ALIAS);
        }
    }

    @Test
    void deleteKek() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            manager.generateKek(ALIAS);
            assertThat(manager.exists(ALIAS)).isTrue();

            manager.deleteKek(ALIAS);
            assertThat(manager.exists(ALIAS)).isFalse();
        }
    }
}
