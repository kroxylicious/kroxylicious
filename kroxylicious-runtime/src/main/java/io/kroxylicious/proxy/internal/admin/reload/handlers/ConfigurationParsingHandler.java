/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload.handlers;

import java.util.Objects;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestContext;
import io.kroxylicious.proxy.internal.admin.reload.ValidationException;

/**
 * Parses the YAML request body into a Configuration object.
 */
public class ConfigurationParsingHandler implements ReloadRequestHandler {

    private final ConfigParser parser;

    public ConfigurationParsingHandler(ConfigParser parser) {
        this.parser = Objects.requireNonNull(parser, "parser cannot be null");
    }

    @Override
    public ReloadRequestContext handle(ReloadRequestContext context) throws ValidationException {
        String yamlBody = context.getRequestBody();

        if (yamlBody == null || yamlBody.trim().isEmpty()) {
            throw new ValidationException("Request body is empty");
        }

        try {
            Configuration config = parser.parseConfiguration(yamlBody);
            return context.withParsedConfiguration(config);
        }
        catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid configuration YAML: " + e.getMessage(), e);
        }
        catch (Exception e) {
            throw new ValidationException("Failed to parse configuration: " + e.getMessage(), e);
        }
    }
}
