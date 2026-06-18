/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.proxy.internal.util.Metrics;

import static io.micrometer.core.instrument.Metrics.globalRegistry;
import static org.assertj.core.api.Assertions.assertThat;

class VirtualClusterLifecycleMetricsTest {

    private static final String CLUSTER = "metrics-cluster";
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);

    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void addMeterRegistry() {
        meterRegistry = new SimpleMeterRegistry();
        globalRegistry.add(meterRegistry);
    }

    @AfterEach
    void removeMeterRegistry() {
        meterRegistry.getMeters().forEach(globalRegistry::remove);
        globalRegistry.remove(meterRegistry);
        // The state-set gauge is backed by a static cache; clear it so each test starts fresh.
        Metrics.clear();
    }

    @Test
    void stateGaugeIsDeferredUntilFirstTransition() {
        // when — a lifecycle is constructed but no transition has happened yet
        new VirtualClusterLifecycle(CLUSTER, DRAIN_TIMEOUT);

        // then — no state series is registered yet. Emission is deferred to the first transition
        // so it happens after the proxy's common-tags micrometer hook is installed (see
        // VirtualClusterLifecycle constructor).
        assertThat(meterRegistry.find("kroxylicious_virtual_cluster_state")
                .tag("virtual_cluster", CLUSTER).gauges()).isEmpty();
    }

    @Test
    void transitionMovesStateSetGaugeToNewState() {
        // given
        var lifecycle = new VirtualClusterLifecycle(CLUSTER, DRAIN_TIMEOUT);

        // when
        lifecycle.initializationSucceeded();

        // then — the active state reads 1 and the state just left reads 0.
        assertThat(stateGauge("serving")).isEqualTo(1.0);
        assertThat(stateGauge("initializing")).isEqualTo(0.0);
    }

    @Test
    void transitionIncrementsTransitionsCounter() {
        // given
        var lifecycle = new VirtualClusterLifecycle(CLUSTER, DRAIN_TIMEOUT);

        // when
        lifecycle.initializationSucceeded();

        // then
        assertThat(meterRegistry.get("kroxylicious_virtual_cluster_transitions_total")
                .tags("virtual_cluster", CLUSTER, "from", "initializing", "to", "serving")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void transitionRecordsTimeSpentInStateBeingLeft() {
        // given
        var lifecycle = new VirtualClusterLifecycle(CLUSTER, DRAIN_TIMEOUT);

        // when
        lifecycle.initializationSucceeded();

        // then — one sample recorded against the initializing state it just left.
        assertThat(meterRegistry.get("kroxylicious_virtual_cluster_state_duration_seconds")
                .tags("virtual_cluster", CLUSTER, "state", "initializing")
                .timer().count()).isEqualTo(1L);
    }

    @Test
    void idempotentStopDoesNotRecordPhantomTransition() {
        // given — a lifecycle already driven to its terminal Stopped state
        var lifecycle = new VirtualClusterLifecycle(CLUSTER, DRAIN_TIMEOUT);
        lifecycle.initializationFailed(new RuntimeException("boom"));
        lifecycle.stop();

        // when — stop() is called again while already Stopped (an idempotent no-op)
        lifecycle.stop();

        // then — the no-op self-transition is not recorded as a transition.
        assertThat(meterRegistry.find("kroxylicious_virtual_cluster_transitions_total")
                .tags("virtual_cluster", CLUSTER, "from", "stopped", "to", "stopped")
                .counter()).isNull();
    }

    @Test
    void stateDurationReflectsTimeSpentInState() {
        // given — a serving cluster and a MockClock so the recorded duration is deterministic
        // rather than wall-clock.
        var clock = new MockClock();
        var lifecycle = new VirtualClusterLifecycle(CLUSTER, DRAIN_TIMEOUT, clock);
        lifecycle.initializationSucceeded(); // enters serving at the mock clock's t0

        // when — two seconds elapse in the serving state, then it transitions out
        clock.add(Duration.ofSeconds(2));
        lifecycle.startDraining();

        // then — the serving state's recorded duration reflects the elapsed time.
        assertThat(meterRegistry.get("kroxylicious_virtual_cluster_state_duration_seconds")
                .tags("virtual_cluster", CLUSTER, "state", "serving")
                .timer().totalTime(TimeUnit.SECONDS)).isGreaterThanOrEqualTo(2.0);
    }

    private double stateGauge(String state) {
        return meterRegistry.get("kroxylicious_virtual_cluster_state")
                .tags("virtual_cluster", CLUSTER, "state", state)
                .gauge().value();
    }
}
