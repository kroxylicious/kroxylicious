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

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Rate-limits warning emissions per {@link ApiKeys}, so a recurring formatting
 * failure does not flood the log.
 *
 * <p>Not thread-safe — intended for use on a single Netty event-loop thread.
 */
class LogWarningThrottle {

    static final Duration SUPPRESSION_INTERVAL = Duration.ofMinutes(5);

    private final Clock clock;
    private final Map<ApiKeys, FailureState> states = new EnumMap<>(ApiKeys.class);

    LogWarningThrottle(Clock clock) {
        this.clock = clock;
    }

    void onFailure(ApiKeys apiKey, short apiVersion, Exception exception, Logger logger) {
        FailureState state = states.get(apiKey);
        long now = clock.millis();
        if (state == null) {
            states.put(apiKey, new FailureState(now));
            warnFirst(logger, apiKey, apiVersion, exception);
        }
        else if (now - state.lastWarnedMillis >= SUPPRESSION_INTERVAL.toMillis()) {
            long suppressed = state.suppressedCount;
            state.lastWarnedMillis = now;
            state.suppressedCount = 0;
            warnRecurring(logger, apiKey, apiVersion, exception, suppressed);
        }
        else {
            state.suppressedCount++;
        }
    }

    private static void warnFirst(Logger logger, ApiKeys apiKey, short apiVersion, Exception exception) {
        logger.atLevel(Level.WARN)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("error", exception.getMessage())
                .setCause(logger.isDebugEnabled() ? exception : null)
                .log(logger.isDebugEnabled()
                        ? "failed to log protocol entry"
                        : "failed to log protocol entry, increase log level to DEBUG for stacktrace");
    }

    private static void warnRecurring(Logger logger, ApiKeys apiKey, short apiVersion, Exception exception, long suppressedCount) {
        logger.atLevel(Level.WARN)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("error", exception.getMessage())
                .addKeyValue("suppressedCount", suppressedCount)
                .setCause(logger.isDebugEnabled() ? exception : null)
                .log(logger.isDebugEnabled()
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
