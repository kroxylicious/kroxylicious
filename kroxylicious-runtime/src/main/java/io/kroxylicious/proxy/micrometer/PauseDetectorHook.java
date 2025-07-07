/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.beans.ConstructorProperties;
import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.pause.ClockDriftPauseDetector;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;

import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = PauseDetectorHook.PauseDetectorHookConfig.class)
public class PauseDetectorHook implements MicrometerConfigurationHookService<PauseDetectorHook.PauseDetectorHookConfig> {

    private static final Logger log = LoggerFactory.getLogger(PauseDetectorHook.class);

    @NonNull
    @Override
    public MicrometerConfigurationHook build(PauseDetectorHookConfig config) {
        return new Hook(config);
    }

    public static class PauseDetectorHookConfig {

        // 100ms is the micrometer recommended default
        static final long DEFAULT_SLEEP_INTERVAL_MS = 100;

        // 100ms is the micrometer recommended default
        static final long DEFAULT_PAUSE_THRESHOLD_MS = 100;

        private final Duration sleepIntervalMs;

        private final Duration pauseThresholdMs;

        @JsonCreator
        @ConstructorProperties({ "sleepIntervalMs", "pauseThresholdMs" })
        public PauseDetectorHookConfig(final Long sleepIntervalMs, final Long pauseThresholdMs) {
            this.sleepIntervalMs = Duration.ofMillis(sleepIntervalMs != null ? sleepIntervalMs : DEFAULT_SLEEP_INTERVAL_MS);
            this.pauseThresholdMs = Duration.ofMillis(pauseThresholdMs != null ? pauseThresholdMs : DEFAULT_PAUSE_THRESHOLD_MS);
        }

        public Duration getSleepInterval() {
            return sleepIntervalMs;
        }

        public Duration getPauseThreshold() {
            return pauseThresholdMs;
        }
    }

    private record Hook(@NonNull PauseDetectorHookConfig config) implements MicrometerConfigurationHook {

        private Hook {
            Objects.requireNonNull(config, "config must be non null");
        }

        @Override
        public void configure(MeterRegistry targetRegistry) {
            final PauseDetector pauseDetector = new ClockDriftPauseDetector(config.getSleepInterval(), config.getPauseThreshold());
            targetRegistry.config().pauseDetector(pauseDetector);
            log.info("configured pause detector on micrometer registry with sleep interval: {} and pause threshold: {}", config.getSleepInterval(),
                    config.getPauseThreshold());
        }
    }

}
