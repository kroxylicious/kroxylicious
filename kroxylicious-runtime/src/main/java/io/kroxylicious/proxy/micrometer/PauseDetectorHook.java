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

/**
 * A {@link MicrometerConfigurationHookService} that configures a {@link ClockDriftPauseDetector}
 * on the meter registry so that timer measurements can compensate for JVM pauses.
 */
@Plugin(configType = PauseDetectorHook.PauseDetectorHookConfig.class)
public class PauseDetectorHook implements MicrometerConfigurationHookService<PauseDetectorHook.PauseDetectorHookConfig> {

    private static final Logger log = LoggerFactory.getLogger(PauseDetectorHook.class);

    /**
     * Creates a new service instance, invoked by the plugin framework.
     */
    public PauseDetectorHook() {
        // nothing to initialise: state is created in build(PauseDetectorHookConfig)
    }

    @Override
    public MicrometerConfigurationHook build(PauseDetectorHookConfig config) {
        return new Hook(config);
    }

    // The raw millisecond components are kept nullable so that "unset" is distinct from an explicit
    // value for equality (used by reconfigure's static-section diff); the getters apply defaults.
    /**
     * Configuration for the {@link PauseDetectorHook}.
     *
     * @param sleepIntervalMs sleep interval of the clock drift pause detector, in milliseconds; null means the default of 100ms.
     * @param pauseThresholdMs pause threshold of the clock drift pause detector, in milliseconds; null means the default of 100ms.
     */
    public record PauseDetectorHookConfig(@Nullable Long sleepIntervalMs, @Nullable Long pauseThresholdMs) {

        // 100ms is the micrometer recommended default
        static final long DEFAULT_SLEEP_INTERVAL_MS = 100;

        // 100ms is the micrometer recommended default
        static final long DEFAULT_PAUSE_THRESHOLD_MS = 100;

        /**
         * The sleep interval to use, applying the default if none was configured.
         *
         * @return the sleep interval.
         */
        public Duration getSleepInterval() {
            return Duration.ofMillis(sleepIntervalMs != null ? sleepIntervalMs : DEFAULT_SLEEP_INTERVAL_MS);
        }

        /**
         * The pause threshold to use, applying the default if none was configured.
         *
         * @return the pause threshold.
         */
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
