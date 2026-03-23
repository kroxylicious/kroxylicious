/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Slf4jEmitterFactoryTest {

    @Test
    void shouldCopeWithNullConfig() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        assertThat(slf4jEmitterFactory.create(null)).isNotNull();
    }

    @Test
    void shouldCopeWithMinimalConfig() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        assertThat(slf4jEmitterFactory.create(new SlfEmitterConfig(Level.DEBUG, List.of()))).isNotNull();
    }

    @Test
    void shouldCopeWithLogAt() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        SlfEmitterConfig config = new SlfEmitterConfig(Level.DEBUG, List.of(
                new LevelExceptionConfig("Foo", Level.DEBUG, null, null)));
        assertThat(slf4jEmitterFactory.create(config)).isNotNull();
    }

    @Test
    void shouldCopeWithLogSuccessfulAt() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        SlfEmitterConfig config = new SlfEmitterConfig(Level.DEBUG, List.of(
                new LevelExceptionConfig("Foo", null, Level.DEBUG, null)));
        assertThat(slf4jEmitterFactory.create(config)).isNotNull();
    }

    @Test
    void shouldCopeWithLogFailureAt() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        SlfEmitterConfig config = new SlfEmitterConfig(Level.DEBUG, List.of(
                new LevelExceptionConfig("Foo", null, null, Level.DEBUG)));
        assertThat(slf4jEmitterFactory.create(config)).isNotNull();
    }

    @Test
    void shouldCopeWithLogSuccessfulAtAndLogFailureAt() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        SlfEmitterConfig config = new SlfEmitterConfig(Level.DEBUG, List.of(
                new LevelExceptionConfig("Foo", null, Level.INFO, Level.DEBUG)));
        assertThat(slf4jEmitterFactory.create(config)).isNotNull();
    }

    @SuppressWarnings("resource")
    @Test
    void shouldThrowOnDuplicateAction() {
        Slf4jEmitterFactory slf4jEmitterFactory = new Slf4jEmitterFactory();
        SlfEmitterConfig config = new SlfEmitterConfig(Level.DEBUG, List.of(
                new LevelExceptionConfig("Foo", null, Level.INFO, Level.DEBUG),
                new LevelExceptionConfig("Foo", null, Level.INFO, Level.DEBUG)));
        assertThatThrownBy(() -> slf4jEmitterFactory.create(config))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate actions found in the `except` property of the Slf4JEmitter: [Foo]");
    }
}