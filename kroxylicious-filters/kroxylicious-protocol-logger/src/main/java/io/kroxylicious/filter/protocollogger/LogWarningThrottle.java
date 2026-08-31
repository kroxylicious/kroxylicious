/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Rate-limits warning emissions per {@link ApiKeys}, so a recurring formatting
 * failure does not flood the log.
 *
 * <p>Not thread-safe — intended for use on a single Netty event-loop thread.
 */
class LogWarningThrottle {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogWarningThrottle.class);

    static final Duration SUPPRESSION_INTERVAL = Duration.ofMinutes(5);

    private final Clock clock;
    private final String targetLoggerName;
    private final Map<ApiKeys, FailureState> states = new EnumMap<>(ApiKeys.class);

    LogWarningThrottle(Clock clock, String targetLoggerName) {
        this.clock = clock;
        this.targetLoggerName = targetLoggerName;
    }

    void onFailure(ApiKeys apiKey, short apiVersion, Exception exception) {
        FailureState state = states.get(apiKey);
        long now = clock.millis();
        if (state == null) {
            states.put(apiKey, new FailureState(now));
            warnFirst(apiKey, apiVersion, exception);
        }
        else if (now - state.lastWarnedMillis >= SUPPRESSION_INTERVAL.toMillis()) {
            long suppressed = state.suppressedCount;
            state.lastWarnedMillis = now;
            state.suppressedCount = 0;
            warnRecurring(apiKey, apiVersion, exception, suppressed);
        }
        else {
            state.suppressedCount++;
        }
    }

    private void warnFirst(ApiKeys apiKey, short apiVersion, Exception exception) {
        LOGGER.atLevel(Level.WARN)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("targetLogger", targetLoggerName)
                .addKeyValue("error", exception.getMessage())
                .setCause(LOGGER.isDebugEnabled() ? exception : null)
                .log(LOGGER.isDebugEnabled()
                        ? "failed to log protocol entry"
                        : "failed to log protocol entry, increase log level to DEBUG for stacktrace");
    }

    private void warnRecurring(ApiKeys apiKey, short apiVersion, Exception exception, long suppressedCount) {
        LOGGER.atLevel(Level.WARN)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("targetLogger", targetLoggerName)
                .addKeyValue("error", exception.getMessage())
                .addKeyValue("suppressedCount", suppressedCount)
                .setCause(LOGGER.isDebugEnabled() ? exception : null)
                .log(LOGGER.isDebugEnabled()
                        ? "failed to log protocol entry"
                        : "failed to log protocol entry, increase log level to DEBUG for stacktrace");
    }

    private static class FailureState {
        long lastWarnedMillis;
        long suppressedCount;

        FailureState(long lastWarnedMillis) {
            this.lastWarnedMillis = lastWarnedMillis;
            this.suppressedCount = 0;
        }
    }
}
