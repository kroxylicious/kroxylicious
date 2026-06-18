/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.pause.ClockDriftPauseDetector;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;

import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = PauseDetectorHook.PauseDetectorHookConfig.class)
public class PauseDetectorHook implements MicrometerConfigurationHookService<PauseDetectorHook.PauseDetectorHookConfig> {

    private static final Logger log = LoggerFactory.getLogger(PauseDetectorHook.class);

    @Override
    public MicrometerConfigurationHook build(PauseDetectorHookConfig config) {
        return new Hook(config);
    }

    // The raw millisecond components are kept nullable so that "unset" is distinct from an explicit
    // value for equality (used by reconfigure's static-section diff); the getters apply defaults.
    public record PauseDetectorHookConfig(@Nullable Long sleepIntervalMs, @Nullable Long pauseThresholdMs) {

        // 100ms is the micrometer recommended default
        static final long DEFAULT_SLEEP_INTERVAL_MS = 100;

        // 100ms is the micrometer recommended default
        static final long DEFAULT_PAUSE_THRESHOLD_MS = 100;

        public Duration getSleepInterval() {
            return Duration.ofMillis(sleepIntervalMs != null ? sleepIntervalMs : DEFAULT_SLEEP_INTERVAL_MS);
        }

        public Duration getPauseThreshold() {
            return Duration.ofMillis(pauseThresholdMs != null ? pauseThresholdMs : DEFAULT_PAUSE_THRESHOLD_MS);
        }
    }

    private record Hook(PauseDetectorHookConfig config) implements MicrometerConfigurationHook {

        private Hook {
            Objects.requireNonNull(config, "config must be non null");
        }

        @Override
        public void configure(MeterRegistry targetRegistry) {
            final PauseDetector pauseDetector = new ClockDriftPauseDetector(config.getSleepInterval(), config.getPauseThreshold());
            targetRegistry.config().pauseDetector(pauseDetector);
            log.atInfo()
                    .addKeyValue("sleepIntervalMs", config.getSleepInterval().toMillis())
                    .addKeyValue("pauseThresholdMs", config.getPauseThreshold().toMillis())
                    .log("Configured pause detector on micrometer registry");
        }
    }

}
