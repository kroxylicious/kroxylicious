/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static io.kroxylicious.proxy.internal.VirtualClusterState.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualClusterStateMachineTest {

    private static final String CLUSTER_NAME = "test-cluster";

    @Test
    void shouldStartInInitializingState() {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);

        // Then
        assertThat(lifecycle.currentState()).isEqualTo(INITIALIZING);
    }

    @Test
    void shouldExposeClusterName() {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);

        // Then
        assertThat(lifecycle.clusterName()).isEqualTo(CLUSTER_NAME);
    }

    static Stream<Arguments> validTransitions() {
        return Stream.of(
                Arguments.of(INITIALIZING, SERVING),
                Arguments.of(INITIALIZING, FAILED),
                Arguments.of(SERVING, DRAINING),
                Arguments.of(DRAINING, INITIALIZING),
                Arguments.of(FAILED, INITIALIZING));
    }

    @ParameterizedTest
    @MethodSource("validTransitions")
    void shouldAllowValidTransition(VirtualClusterState from, VirtualClusterState to) {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);
        advanceTo(lifecycle, from);

        // When
        lifecycle.transitionTo(to);

        // Then
        assertThat(lifecycle.currentState()).isEqualTo(to);
    }

    static Stream<Arguments> invalidTransitions() {
        return Stream.of(
                Arguments.of(INITIALIZING, INITIALIZING),
                Arguments.of(INITIALIZING, DRAINING),
                Arguments.of(SERVING, INITIALIZING),
                Arguments.of(SERVING, SERVING),
                Arguments.of(SERVING, FAILED),
                Arguments.of(DRAINING, SERVING),
                Arguments.of(DRAINING, DRAINING),
                Arguments.of(DRAINING, FAILED),
                Arguments.of(FAILED, SERVING),
                Arguments.of(FAILED, DRAINING),
                Arguments.of(FAILED, FAILED));
    }

    @ParameterizedTest
    @MethodSource("invalidTransitions")
    void shouldRejectInvalidTransition(VirtualClusterState from, VirtualClusterState to) {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);
        advanceTo(lifecycle, from);

        // Then
        assertThatThrownBy(() -> lifecycle.transitionTo(to))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(CLUSTER_NAME)
                .hasMessageContaining(from.name())
                .hasMessageContaining(to.name());
    }

    @Test
    void shouldRejectNullState() {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);

        // Then
        assertThatThrownBy(() -> lifecycle.transitionTo(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectNullClusterName() {
        assertThatThrownBy(() -> new VirtualClusterStateMachine(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldSupportFullReloadCycle() {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);

        // When — simulate: start → serve → drain for reload → re-init → serve again
        lifecycle.transitionTo(SERVING);
        lifecycle.transitionTo(DRAINING);
        lifecycle.transitionTo(INITIALIZING);
        lifecycle.transitionTo(SERVING);

        // Then
        assertThat(lifecycle.currentState()).isEqualTo(SERVING);
    }

    @Test
    void shouldSupportRetryAfterFailure() {
        // Given
        var lifecycle = new VirtualClusterStateMachine(CLUSTER_NAME);

        // When — simulate: fail on init → retry → succeed
        lifecycle.transitionTo(FAILED);
        lifecycle.transitionTo(INITIALIZING);
        lifecycle.transitionTo(SERVING);

        // Then
        assertThat(lifecycle.currentState()).isEqualTo(SERVING);
    }

    /**
     * Advances the lifecycle to the target state via the shortest valid path from INITIALIZING.
     */
    private void advanceTo(VirtualClusterStateMachine lifecycle, VirtualClusterState target) {
        switch (target) {
            case INITIALIZING -> {
                // already there
            }
            case SERVING -> lifecycle.transitionTo(SERVING);
            case DRAINING -> {
                lifecycle.transitionTo(SERVING);
                lifecycle.transitionTo(DRAINING);
            }
            case FAILED -> lifecycle.transitionTo(FAILED);
        }
    }
}