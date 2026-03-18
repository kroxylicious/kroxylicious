/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.assertj.core.api.Condition;
import org.assertj.core.condition.Not;

import io.kroxylicious.proxy.config.AuditEmitterConfigBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;

/**
 * Utility class providing shared infrastructure for audit logging tests.
 * <p>
 * This class provides:
 * <ul>
 *   <li>LogCaptor - captures Log4j events from the "audit" logger</li>
 *   <li>Assertion helpers - conditions for validating audit log events</li>
 *   <li>Configuration helpers - methods to add audit emitters to test configurations</li>
 * </ul>
 */
public class AuditLoggingTestSupport {

    private AuditLoggingTestSupport() {
        // Utility class
    }

    /**
     * Captures Log4j events from the "audit" logger for test assertions.
     * Must be used within try-with-resources to ensure proper cleanup.
     */
    public static class LogCaptor implements AutoCloseable {
        private final ListAppender appender;
        private final LoggerContext context;
        private final Configuration config;

        public LogCaptor() {
            context = (LoggerContext) LogManager.getContext(false);
            config = context.getConfiguration();
            // Create and start the ListAppender
            appender = ListAppender.newBuilder()
                    .setName("TestAppender")
                    .build();
            appender.start();

            // Attach it to the root configuration (or a specific logger)
            config.addAppender(appender);
            config.getLoggerConfig("audit").addAppender(appender, Level.ALL, null);
            // config.getRootLogger().addAppender(appender, Level.ALL, null);

            // Update the context to apply changes
            context.updateLoggers();
        }

        @Override
        public void close() {
            // Stop and remove the appender to prevent side effects in other tests
            appender.stop();
            config.getLoggerConfig("audit").removeAppender("TestAppender");
            context.updateLoggers();
        }

        public List<LogEvent> capturedEvents() {
            return appender.getEvents();
        }
    }

    /**
     * Returns a condition that matches successful audit events (no status field).
     */
    public static Condition<LogEvent> isSuccess() {
        return Not.not(isFailure());
    }

    /**
     * Returns a condition that matches failed audit events (contains status field).
     */
    public static Condition<LogEvent> isFailure() {
        return new Condition<>(e -> e.getMessage().getFormattedMessage().contains("\"status\":\""),
                "isFailure");
    }

    /**
     * Returns a condition that matches audit events with the specified action name.
     */
    public static Condition<LogEvent> auditAction(String action) {
        return new Condition<>(e -> e.getMessage().getFormattedMessage().contains("\"action\":\"" + action + "\""),
                action + " action");
    }

    /**
     * Adds only Slf4j audit logging emitter to the configuration.
     * Use this for tests that focus purely on audit logging without metrics.
     *
     * @param builder the configuration builder to modify
     * @return the same builder for chaining
     */
    public static ConfigurationBuilder addAuditLogging(ConfigurationBuilder builder) {
        builder.withNewAudit()
                .addToEmitters(new AuditEmitterConfigBuilder("logging-emitter", "io.kroxylicious.audit.emitter.slf4j.Slf4jEmitterFactory").build())
                .endAudit();
        return builder;
    }

    /**
     * Adds both Metrics and Slf4j audit emitters to the configuration.
     * Use this for tests that validate both audit metrics and logging work together.
     *
     * @param builder the configuration builder to modify
     * @return the same builder for chaining
     */
    public static ConfigurationBuilder addAuditMetricsAndLogging(ConfigurationBuilder builder) {
        builder.withNewAudit()
                .addToEmitters(new AuditEmitterConfigBuilder("metric-emitter", "io.kroxylicious.audit.emitter.metrics.MetricsEmitterFactory").build())
                .addToEmitters(new AuditEmitterConfigBuilder("logging-emitter", "io.kroxylicious.audit.emitter.slf4j.Slf4jEmitterFactory").build())
                .endAudit();
        return builder;
    }
}
