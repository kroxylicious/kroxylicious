/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import java.time.Duration;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.pause.ClockDriftPauseDetector;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;

import static io.kroxylicious.proxy.micrometer.PauseDetectorHook.PauseDetectorHookConfig.DEFAULT_PAUSE_THRESHOLD_MS;
import static io.kroxylicious.proxy.micrometer.PauseDetectorHook.PauseDetectorHookConfig.DEFAULT_SLEEP_INTERVAL_MS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PauseDetectorHookTest {

    @Test
    void build_whenGivenConfigIsNull_shouldThrowException() {
        final PauseDetectorHook pauseDetectorHook = new PauseDetectorHook();
        // noinspection resource
        assertThatThrownBy(() -> pauseDetectorHook.build(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configure_whenConfigIsGiven_shouldUseGivenValue() {
        final long sleepIntervalMs = 250;
        final long pauseThresholdMs = 250;
        final PauseDetectorHook.PauseDetectorHookConfig config = new PauseDetectorHook.PauseDetectorHookConfig(sleepIntervalMs, pauseThresholdMs);
        try (MicrometerConfigurationHook hook = new PauseDetectorHook().build(config)) {
            final MeterRegistry meterRegistry = givenRegistryConfiguredWith(hook);
            final PauseDetector pauseDetector = meterRegistry.config().pauseDetector();
            thenPauseDetectorEquals(pauseDetector, sleepIntervalMs, pauseThresholdMs);
        }
    }

    @Test
    void configure_whenConfigIsNotGiven_shouldUseDefaults() {
        final PauseDetectorHook.PauseDetectorHookConfig config = new PauseDetectorHook.PauseDetectorHookConfig(null, null);
        try (MicrometerConfigurationHook hook = new PauseDetectorHook().build(config)) {
            final MeterRegistry meterRegistry = givenRegistryConfiguredWith(hook);
            final PauseDetector pauseDetector = meterRegistry.config().pauseDetector();
            thenPauseDetectorEquals(pauseDetector, DEFAULT_SLEEP_INTERVAL_MS, DEFAULT_PAUSE_THRESHOLD_MS);
        }
    }

    private static MeterRegistry givenRegistryConfiguredWith(MicrometerConfigurationHook hook) {
        MeterRegistry registry = new CompositeMeterRegistry();
        hook.configure(registry);
        return registry;
    }

    private static void thenPauseDetectorEquals(final PauseDetector pauseDetector, final long sleepInterval, final long pauseThreshold) {
        assertThat(pauseDetector).isInstanceOf(ClockDriftPauseDetector.class);
        final ClockDriftPauseDetector clockDriftPauseDetector = (ClockDriftPauseDetector) pauseDetector;
        assertThat(clockDriftPauseDetector.getPauseThreshold()).isEqualTo(Duration.ofMillis(sleepInterval));
        assertThat(clockDriftPauseDetector.getSleepInterval()).isEqualTo(Duration.ofMillis(pauseThreshold));
    }
}