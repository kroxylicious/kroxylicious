/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogging;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.event.Level;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * A {@link FilterFactory} for {@link ProtocolLoggingFilter}.
 */
@Plugin(configType = ProtocolLogging.Config.class)
public class ProtocolLogging implements FilterFactory<ProtocolLogging.Config, ProtocolLogging.Config> {

    static final int DEFAULT_MAX_BODY_CHARS = 8192;
    static final Level DEFAULT_LOG_LEVEL = Level.DEBUG;

    /**
     * Default constructor used by the runtime.
     */
    public ProtocolLogging() {
        // required for Javadoc and runtime plugin instantiation
    }

    /**
     * Configuration for the protocol logging filter.
     *
     * @param apiKeyNames Kafka {@link org.apache.kafka.common.protocol.ApiKeys} enum names to log.
     *                    Absent or empty means all API keys.
     * @param maxBodyChars maximum characters in the JSON body before truncation (default 8192, must be positive).
     * @param logLevel the SLF4J level at which the filter emits log messages (default {@link Level#DEBUG}).
     */
    public record Config(@JsonProperty List<String> apiKeyNames,
                         @JsonProperty Integer maxBodyChars,
                         @JsonProperty Level logLevel) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        List<String> apiKeyNames = (config != null && config.apiKeyNames() != null) ? config.apiKeyNames() : List.of();
        int maxBodyChars = (config != null && config.maxBodyChars() != null) ? config.maxBodyChars() : DEFAULT_MAX_BODY_CHARS;
        Level logLevel = (config != null && config.logLevel() != null) ? config.logLevel() : DEFAULT_LOG_LEVEL;

        for (String name : apiKeyNames) {
            try {
                ApiKeys.valueOf(name);
            }
            catch (IllegalArgumentException e) {
                throw new PluginConfigurationException(
                        "Unknown API key name '" + name + "' in apiKeyNames. Must be a valid org.apache.kafka.common.protocol.ApiKeys name.");
            }
        }

        if (maxBodyChars <= 0) {
            throw new PluginConfigurationException("maxBodyChars must be greater than zero, got " + maxBodyChars);
        }

        return new Config(apiKeyNames, maxBodyChars, logLevel);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        Set<ApiKeys> keys;
        if (config.apiKeyNames().isEmpty()) {
            keys = EnumSet.allOf(ApiKeys.class);
        }
        else {
            keys = config.apiKeyNames().stream()
                    .map(ApiKeys::valueOf)
                    .collect(Collectors.toCollection(() -> EnumSet.noneOf(ApiKeys.class)));
        }
        return new ProtocolLoggingFilter(keys, new MessageFormatter(config.maxBodyChars()), config.logLevel());
    }

}
