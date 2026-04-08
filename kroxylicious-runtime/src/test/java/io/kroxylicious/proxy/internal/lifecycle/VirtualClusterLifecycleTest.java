/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.lifecycle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.DRAINING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.FAILED;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.INITIALIZING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.SERVING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualClusterLifecycleTest {

    private VirtualClusterLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        lifecycle = new VirtualClusterLifecycle("test-cluster");
    }

    @Test
    void initialStateIsInitializing() {
        assertThat(lifecycle.state()).isEqualTo(INITIALIZING);
        assertThat(lifecycle.clusterName()).isEqualTo("test-cluster");
    }

    @Test
    void initializingToServingIsLegal() {
        lifecycle.transitionTo(SERVING);
        assertThat(lifecycle.state()).isEqualTo(SERVING);
    }

    @Test
    void initializingToFailedIsLegal() {
        var cause = new RuntimeException("bind failed");
        lifecycle.transitionToFailed(cause);
        assertThat(lifecycle.state()).isEqualTo(FAILED);
        assertThat(lifecycle.failureCause()).contains(cause);
    }

    @Test
    void servingToDrainingIsLegal() {
        lifecycle.transitionTo(SERVING);
        lifecycle.transitionTo(DRAINING);
        assertThat(lifecycle.state()).isEqualTo(DRAINING);
    }

    @Test
    void drainingToStoppedIsLegal() {
        lifecycle.transitionTo(SERVING);
        lifecycle.transitionTo(DRAINING);
        lifecycle.transitionTo(STOPPED);
        assertThat(lifecycle.state()).isEqualTo(STOPPED);
    }

    @Test
    void failedToStoppedIsLegal() {
        lifecycle.transitionToFailed(new RuntimeException("oops"));
        lifecycle.transitionTo(STOPPED);
        assertThat(lifecycle.state()).isEqualTo(STOPPED);
    }

    @Test
    void transitionToFailedClearsOnSubsequentTransition() {
        lifecycle.transitionToFailed(new RuntimeException("oops"));
        assertThat(lifecycle.failureCause()).isPresent();

        lifecycle.transitionTo(STOPPED);
        assertThat(lifecycle.failureCause()).isEmpty();
    }

    // Illegal transitions

    @Test
    void initializingToStoppedIsIllegal() {
        assertThatThrownBy(() -> lifecycle.transitionTo(STOPPED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("INITIALIZING")
                .hasMessageContaining("STOPPED");
    }

    @Test
    void initializingToDrainingIsIllegal() {
        assertThatThrownBy(() -> lifecycle.transitionTo(DRAINING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void servingToInitializingIsIllegal() {
        lifecycle.transitionTo(SERVING);
        assertThatThrownBy(() -> lifecycle.transitionTo(INITIALIZING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void servingToStoppedIsIllegal() {
        lifecycle.transitionTo(SERVING);
        assertThatThrownBy(() -> lifecycle.transitionTo(STOPPED))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void stoppedToAnyIsIllegal() {
        lifecycle.transitionTo(SERVING);
        lifecycle.transitionTo(DRAINING);
        lifecycle.transitionTo(STOPPED);

        assertThatThrownBy(() -> lifecycle.transitionTo(SERVING))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> lifecycle.transitionTo(INITIALIZING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failedToServingIsIllegal() {
        lifecycle.transitionToFailed(new RuntimeException("oops"));
        assertThatThrownBy(() -> lifecycle.transitionTo(SERVING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void transitionToFailedFromServingIsIllegal() {
        lifecycle.transitionTo(SERVING);
        assertThatThrownBy(() -> lifecycle.transitionToFailed(new RuntimeException("oops")))
                .isInstanceOf(IllegalStateException.class);
    }

    // isAcceptingConnections

    @Test
    void isAcceptingConnectionsOnlyInServing() {
        assertThat(lifecycle.isAcceptingConnections()).isFalse(); // INITIALIZING

        lifecycle.transitionTo(SERVING);
        assertThat(lifecycle.isAcceptingConnections()).isTrue();

        lifecycle.transitionTo(DRAINING);
        assertThat(lifecycle.isAcceptingConnections()).isFalse();

        lifecycle.transitionTo(STOPPED);
        assertThat(lifecycle.isAcceptingConnections()).isFalse();
    }

    @Test
    void failureCauseIsEmptyWhenNotFailed() {
        assertThat(lifecycle.failureCause()).isEmpty();
        lifecycle.transitionTo(SERVING);
        assertThat(lifecycle.failureCause()).isEmpty();
    }
}
