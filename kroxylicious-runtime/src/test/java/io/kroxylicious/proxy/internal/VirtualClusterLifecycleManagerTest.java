/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Draining;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Failed;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Initializing;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualClusterLifecycleManagerTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private VirtualClusterLifecycleManager manager;

    @BeforeEach
    void setUp() {
        manager = new VirtualClusterLifecycleManager(CLUSTER_NAME);
    }

    @Test
    void shouldStartInInitializingState() {
        assertThat(manager.state()).isInstanceOf(Initializing.class);
    }

    @Test
    void shouldExposeClusterName() {
        assertThat(manager.clusterName()).isEqualTo(CLUSTER_NAME);
    }

    @Test
    void shouldTransitionToServingOnSuccess() {
        // when
        manager.initializationSucceeded();

        // then
        assertThat(manager.state()).isInstanceOf(Serving.class);
    }

    @Test
    void shouldTransitionToFailedOnError() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        manager.initializationFailed(cause);

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Failed.class, failed -> assertThat(failed.cause()).isSameAs(cause));
    }

    @Test
    void shouldTransitionFromServingToDraining() {
        // given
        manager.initializationSucceeded();

        // when
        manager.startDraining();

        // then
        assertThat(manager.state()).isInstanceOf(Draining.class);
    }

    @Test
    void shouldTransitionFromDrainingToStopped() {
        // given
        manager.initializationSucceeded();
        manager.startDraining();

        // when
        manager.drainComplete();

        // then
        assertThat(manager.state()).isInstanceOf(Stopped.class);
    }

    @Test
    void shouldTransitionFromFailedToStopped() {
        // given
        manager.initializationFailed(new RuntimeException("boom"));

        // when
        manager.stop();

        // then
        assertThat(manager.state()).isInstanceOf(Stopped.class);
    }

    @Test
    void shouldRetainFailureCauseAfterStop() {
        // given
        var cause = new RuntimeException("boom");
        manager.initializationFailed(cause);

        // when
        manager.stop();

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Stopped.class, stopped -> assertThat(stopped.priorFailureCause()).isSameAs(cause));
    }

    @Test
    void shouldTransitionFromInitializingToStoppedOnShutdown() {
        // when
        manager.stop();

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Stopped.class, stopped -> assertThat(stopped.priorFailureCause()).isNull());
    }

    @Test
    void shouldHaveNoPriorFailureCauseWhenStoppedFromDraining() {
        // given
        manager.initializationSucceeded();
        manager.startDraining();

        // when
        manager.drainComplete();

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Stopped.class, stopped -> assertThat(stopped.priorFailureCause()).isNull());
    }

    static Stream<Arguments> invalidTransitions() {
        return Stream.of(
                Arguments.of("initializationSucceeded from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycleManager("c");
                    m.initializationSucceeded();
                    m.initializationSucceeded();
                }),
                Arguments.of("startDraining from INITIALIZING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycleManager("c");
                    m.startDraining();
                }),
                Arguments.of("drainComplete from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycleManager("c");
                    m.initializationSucceeded();
                    m.drainComplete();
                }),
                Arguments.of("stop from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycleManager("c");
                    m.initializationSucceeded();
                    m.stop();
                }),
                Arguments.of("initializationSucceeded from STOPPED", (Runnable) () -> {
                    var m = new VirtualClusterLifecycleManager("c");
                    m.initializationFailed(new RuntimeException("x"));
                    m.stop();
                    m.initializationSucceeded();
                }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTransitions")
    void shouldRejectInvalidTransition(String description, Runnable action) {
        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalStateException.class);
    }
}
