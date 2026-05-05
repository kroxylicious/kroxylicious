/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
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
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class VirtualClusterLifecycleTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);
    private VirtualClusterLifecycle manager;

    @BeforeEach
    void setUp() {
        manager = new VirtualClusterLifecycle(CLUSTER_NAME, DRAIN_TIMEOUT);
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
        VirtualClusterLifecycleState state = manager.state();
        assertThat(state).asInstanceOf(InstanceOfAssertFactories.type(Draining.class))
                .satisfies(draining -> assertThat(draining.drainTimeout()).isEqualTo(DRAIN_TIMEOUT));
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
                argumentSet("initializationSucceeded from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationSucceeded();
                    m.initializationSucceeded();
                }),
                argumentSet("startDraining from INITIALIZING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.startDraining();
                }),
                argumentSet("drainComplete from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationSucceeded();
                    m.drainComplete();
                }),
                argumentSet("stop from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationSucceeded();
                    m.stop();
                }),
                argumentSet("initializationSucceeded from STOPPED", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationFailed(new RuntimeException("x"));
                    m.stop();
                    m.initializationSucceeded();
                }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTransitions")
    void shouldRejectInvalidTransition(Runnable action) {
        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalStateException.class);
    }
}
