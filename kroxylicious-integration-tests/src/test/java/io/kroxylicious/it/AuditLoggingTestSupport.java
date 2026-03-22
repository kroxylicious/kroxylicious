/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.assertj.core.api.Condition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

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
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final JsonSchema AUDIT_SCHEMA;

        static {
            try {
                JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
                InputStream schemaStream = LogCaptor.class.getResourceAsStream("/schemas/audit_action_v1.json");
                if (schemaStream == null) {
                    throw new IllegalStateException("Could not load audit schema from classpath: /schemas/audit_action_v1.json");
                }
                JsonNode schemaNode = MAPPER.readTree(schemaStream);
                AUDIT_SCHEMA = factory.getSchema(schemaNode);
            }
            catch (Exception e) {
                throw new ExceptionInInitializerError("Failed to load audit schema: " + e.getMessage());
            }
        }

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

        public List<JsonNode> capturedEvents() {
            List<LogEvent> events = appender.getEvents();
            List<JsonNode> parsedEvents = new ArrayList<>(events.size());

            for (int i = 0; i < events.size(); i++) {
                LogEvent event = events.get(i);
                String message = event.getMessage().getFormattedMessage();
                try {
                    JsonNode json = MAPPER.readTree(message);

                    // Validate against schema with format assertions enabled
                    Set<ValidationMessage> errors = AUDIT_SCHEMA.validate(json, executionContext -> {
                        executionContext.getExecutionConfig().setFormatAssertionsEnabled(true);
                    });

                    if (!errors.isEmpty()) {
                        String errorDetails = errors.stream()
                                .map(ValidationMessage::toString)
                                .collect(Collectors.joining("\n  - "));
                        throw new IllegalStateException(
                                String.format("Failed to validate audit event at index %d against schema (audit_action_v1).\n" +
                                        "Validation errors:\n  - %s\n" +
                                        "Event JSON: %s",
                                        i, errorDetails, json.toPrettyString()));
                    }

                    parsedEvents.add(json);
                }
                catch (JsonProcessingException e) {
                    throw new IllegalStateException(
                            String.format("Failed to parse audit event at index %d as JSON. " +
                                    "Logger: %s, Message: %s",
                                    i, event.getLoggerName(), message),
                            e);
                }
            }

            return parsedEvents;
        }
    }

    /**
     * Returns a condition that matches successful audit events (no status field).
     */
    public static Condition<JsonNode> isSuccess() {
        return new Condition<>(json -> {
            JsonNode statusNode = json.get("status");
            return statusNode == null || statusNode.isNull();
        }, "is successful audit event");
    }

    /**
     * Returns a condition that matches failed audit events (contains status field).
     */
    public static Condition<JsonNode> isFailure() {
        return new Condition<>(json -> {
            JsonNode statusNode = json.get("status");
            return statusNode != null && !statusNode.isNull();
        }, "is failed audit event");
    }

    /**
     * Returns a condition that matches audit events with the specified action name.
     */
    public static Condition<JsonNode> auditAction(String action) {
        return new Condition<>(json -> {
            JsonNode actionNode = json.get("action");
            return actionNode != null &&
                    actionNode.isTextual() &&
                    action.equals(actionNode.asText());
        }, "action=" + action);
    }

    /**
     * Returns a condition that matches audit events containing the specified objectRef entry.
     * The objectRef maps resource type class names to resource identifiers.
     */
    public static Condition<JsonNode> hasObjectRef(String resourceType, String resourceName) {
        return new Condition<>(json -> {
            JsonNode objectRefNode = json.get("objectRef");
            if (objectRefNode == null || !objectRefNode.isObject()) {
                return false;
            }

            JsonNode resourceNode = objectRefNode.get(resourceType);
            return resourceNode != null &&
                    resourceNode.isTextual() &&
                    resourceName.equals(resourceNode.asText());
        }, "has objectRef " + resourceType + "=" + resourceName);
    }

    /**
     * Returns a condition that matches audit events where the actor has a principal with the specified name.
     * The actor's principals array contains single-property objects where the value is the principal name.
     */
    public static Condition<JsonNode> hasActorPrincipal(String principalName) {
        return new Condition<>(json -> {
            JsonNode actorNode = json.get("actor");
            if (actorNode == null || !actorNode.isObject()) {
                return false;
            }

            JsonNode principalsNode = actorNode.get("principals");
            if (principalsNode == null || !principalsNode.isArray()) {
                return false;
            }

            // Check if any principal in the array has a property value matching the principal name
            for (JsonNode principal : principalsNode) {
                if (principal.isObject()) {
                    // Principals are single-property objects like {"io.kroxylicious.proxy.authentication.User":"alice"}
                    // Check if any property value matches the principalName
                    var fields = principal.fields();
                    while (fields.hasNext()) {
                        var entry = fields.next();
                        JsonNode value = entry.getValue();
                        if (value != null &&
                                value.isTextual() &&
                                principalName.equals(value.asText())) {
                            return true;
                        }
                    }
                }
            }

            return false;
        }, "has actor principal name=" + principalName);
    }

    /**
     * Returns a condition that matches audit events where the actor has no principals.
     * This is true when the principals field is null or missing.
     */
    public static Condition<JsonNode> hasNoPrincipals() {
        return new Condition<>(json -> {
            JsonNode actorNode = json.get("actor");
            if (actorNode == null || !actorNode.isObject()) {
                return false;
            }
            JsonNode principalsNode = actorNode.get("principals");
            return principalsNode == null || principalsNode.isNull();
        }, "has no principals (null or missing)");
    }

    /**
     * Returns a condition that matches audit events where the actor has principals.
     * This is true when the principals field is a non-empty array.
     */
    public static Condition<JsonNode> hasPrincipals() {
        return new Condition<>(json -> {
            JsonNode actorNode = json.get("actor");
            if (actorNode == null || !actorNode.isObject()) {
                return false;
            }
            JsonNode principalsNode = actorNode.get("principals");
            return principalsNode != null && principalsNode.isArray() && principalsNode.size() > 0;
        }, "has non-empty principals array");
    }

    /**
     * Returns a condition that matches audit events where the actor has the specified session ID.
     */
    public static Condition<JsonNode> hasSessionId(String sessionId) {
        return new Condition<>(json -> {
            JsonNode actorNode = json.get("actor");
            if (actorNode == null || !actorNode.isObject()) {
                return false;
            }
            JsonNode sessionNode = actorNode.get("session");
            return sessionNode != null &&
                    sessionNode.isTextual() &&
                    sessionId.equals(sessionNode.asText());
        }, "has session=" + sessionId);
    }

    /**
     * Returns a condition that matches audit events where the actor type is "Client".
     */
    public static Condition<JsonNode> hasClientActorType() {
        return new Condition<>(json -> {
            JsonNode actorNode = json.get("actor");
            if (actorNode == null || !actorNode.isObject()) {
                return false;
            }
            JsonNode typeNode = actorNode.get("type");
            return typeNode != null &&
                    typeNode.isTextual() &&
                    "Client".equals(typeNode.asText());
        }, "has actor type=Client");
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
