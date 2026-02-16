/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

import java.util.List;
import java.util.Objects;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.admin.reload.handlers.ConfigurationParsingHandler;
import io.kroxylicious.proxy.internal.admin.reload.handlers.ConfigurationReloadHandler;
import io.kroxylicious.proxy.internal.admin.reload.handlers.ContentLengthValidationHandler;
import io.kroxylicious.proxy.internal.admin.reload.handlers.ContentTypeValidationHandler;
import io.kroxylicious.proxy.internal.admin.reload.handlers.ReloadRequestHandler;

/**
 * Processes reload requests by chaining multiple handlers using the
 * Chain of Responsibility pattern. Each handler performs a specific task
 * (validation, parsing, execution) and passes the context to the next handler.
 * <p>
 * The processing chain:
 * 1. ContentTypeValidationHandler - Validates Content-Type header
 * 2. ContentLengthValidationHandler - Validates request body size
 * 3. ConfigurationParsingHandler - Parses YAML to Configuration
 * 4. ConfigurationReloadHandler - Executes the reload
 */
public class ReloadRequestProcessor {

    /**
     * Maximum allowed request body size (10MB).
     */
    private static final int MAX_CONTENT_LENGTH = 10 * 1024 * 1024;

    private final List<ReloadRequestHandler> handlers;

    /**
     * Create a processor with the specified parser and orchestrator.
     *
     * @param parser The configuration parser
     * @param orchestrator The reload orchestrator
     * @param timeoutSeconds Timeout in seconds for reload operations
     */
    public ReloadRequestProcessor(
                                  ConfigParser parser,
                                  ConfigurationReloadOrchestrator orchestrator,
                                  long timeoutSeconds) {
        Objects.requireNonNull(parser, "parser cannot be null");
        Objects.requireNonNull(orchestrator, "orchestrator cannot be null");

        this.handlers = List.of(
                new ContentTypeValidationHandler(),
                new ContentLengthValidationHandler(MAX_CONTENT_LENGTH),
                new ConfigurationParsingHandler(parser),
                new ConfigurationReloadHandler(orchestrator, timeoutSeconds));
    }

    /**
     * Process a reload request by passing it through the handler chain.
     *
     * @param context The request context
     * @return The reload response
     * @throws ReloadException If any handler fails
     */
    public ReloadResponse process(ReloadRequestContext context) throws ReloadException {
        ReloadRequestContext currentContext = context;

        for (ReloadRequestHandler handler : handlers) {
            currentContext = handler.handle(currentContext);
        }

        ReloadResponse response = currentContext.getResponse();
        if (response == null) {
            throw new ReloadException("No response generated - ensure ConfigurationReloadHandler is in the chain");
        }

        return response;
    }
}
