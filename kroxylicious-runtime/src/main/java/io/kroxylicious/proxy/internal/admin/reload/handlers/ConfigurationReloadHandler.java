/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload.handlers;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.internal.admin.reload.ConfigurationReloadOrchestrator;
import io.kroxylicious.proxy.internal.admin.reload.ReloadException;
import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestContext;
import io.kroxylicious.proxy.internal.admin.reload.ReloadResponse;
import io.kroxylicious.proxy.internal.admin.reload.ReloadResult;

/**
 * Executes the configuration reload by delegating to the ConfigurationReloadOrchestrator.
 * This handler blocks waiting for the reload to complete (with a timeout).
 */
public class ConfigurationReloadHandler implements ReloadRequestHandler {

    private final ConfigurationReloadOrchestrator orchestrator;
    private final long timeoutSeconds;

    /**
     * Create a handler with the specified orchestrator and timeout.
     *
     * @param orchestrator The reload orchestrator
     * @param timeoutSeconds Timeout in seconds to wait for reload completion
     */
    public ConfigurationReloadHandler(ConfigurationReloadOrchestrator orchestrator, long timeoutSeconds) {
        this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator cannot be null");
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public ReloadRequestContext handle(ReloadRequestContext context) throws ReloadException {
        Configuration config = context.getParsedConfiguration();
        if (config == null) {
            throw new ReloadException("Configuration not parsed - ensure ConfigurationParsingHandler runs before this handler");
        }

        try {
            // Execute reload and wait for completion
            ReloadResult result = orchestrator.reload(config)
                    .get(timeoutSeconds, TimeUnit.SECONDS);

            // Convert result to response
            ReloadResponse response = ReloadResponse.from(result);
            return context.withResponse(response);

        }
        catch (TimeoutException e) {
            throw new ReloadException("Reload timed out after " + timeoutSeconds + " seconds");
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ReloadException reloadException) {
                throw reloadException;
            }
            throw new ReloadException("Reload failed: " + cause.getMessage(), cause);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReloadException("Reload interrupted", e);
        }
    }
}
