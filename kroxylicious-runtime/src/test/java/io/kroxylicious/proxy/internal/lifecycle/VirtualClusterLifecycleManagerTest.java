/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.lifecycle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.DRAINING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.INITIALIZING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.SERVING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualClusterLifecycleManagerTest {

    private VirtualClusterLifecycleManager manager;

    @BeforeEach
    void setUp() {
        manager = new VirtualClusterLifecycleManager();
    }

    @Test
    void registerCreatesLifecycleInInitializingState() {
        var lifecycle = manager.register("cluster-a");
        assertThat(lifecycle).isNotNull();
        assertThat(lifecycle.state()).isEqualTo(INITIALIZING);
        assertThat(lifecycle.clusterName()).isEqualTo("cluster-a");
    }

    @Test
    void registerThrowsOnDuplicate() {
        manager.register("cluster-a");
        assertThatThrownBy(() -> manager.register("cluster-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster-a");
    }

    @Test
    void getReturnsRegisteredLifecycle() {
        var registered = manager.register("cluster-a");
        assertThat(manager.get("cluster-a")).isSameAs(registered);
    }

    @Test
    void getReturnsNullForUnregistered() {
        assertThat(manager.get("unknown")).isNull();
    }

    @Test
    void isAcceptingConnectionsReturnsTrueOnlyWhenServing() {
        var lifecycle = manager.register("cluster-a");

        assertThat(manager.isAcceptingConnections("cluster-a")).isFalse();

        lifecycle.transitionTo(SERVING);
        assertThat(manager.isAcceptingConnections("cluster-a")).isTrue();

        lifecycle.transitionTo(DRAINING);
        assertThat(manager.isAcceptingConnections("cluster-a")).isFalse();
    }

    @Test
    void isAcceptingConnectionsReturnsFalseForUnregistered() {
        assertThat(manager.isAcceptingConnections("unknown")).isFalse();
    }

    @Test
    void allReturnsAllRegisteredLifecycles() {
        manager.register("cluster-a");
        manager.register("cluster-b");
        assertThat(manager.all()).hasSize(2);
    }

    @Test
    void allReturnsUnmodifiableCollection() {
        manager.register("cluster-a");
        assertThatThrownBy(() -> manager.all().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
