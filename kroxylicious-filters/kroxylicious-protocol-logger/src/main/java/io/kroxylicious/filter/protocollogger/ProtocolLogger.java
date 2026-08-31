/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.time.Clock;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link FilterFactory} for {@link ProtocolLoggerFilter}.
 */
@Plugin(configType = ProtocolLogger.Config.class)
public class ProtocolLogger implements FilterFactory<ProtocolLogger.Config, ProtocolLogger.Config> {

    static final Level DEFAULT_LOG_LEVEL = Level.DEBUG;

    private static final Map<String, ApiKeys> NORMALIZED_API_KEY_LOOKUP;

    static {
        NORMALIZED_API_KEY_LOOKUP = EnumSet.allOf(ApiKeys.class).stream()
                .collect(Collectors.toUnmodifiableMap(
                        k -> normalizeKey(k.name()),
                        k -> k));
    }

    /**
     * Default constructor used by the runtime.
     */
    public ProtocolLogger() {
        // required for Javadoc and runtime plugin instantiation
    }

    /**
     * Configuration for the protocol logger filter.
     *
     * @param apiKeyNames Kafka {@link io.kroxylicious.kafka.common.protocol.ApiKeys} enum names to log.
     *                    Absent or empty means all API keys.
     * @param logLevel the SLF4J level at which the filter emits log messages (default {@link Level#DEBUG}).
     * @param loggerName the SLF4J logger name used by this filter instance (default: the filter class name).
     */
    public record Config(@JsonProperty @Nullable Set<String> apiKeyNames,
                         @JsonProperty @Nullable Level logLevel,
                         @JsonProperty @Nullable String loggerName) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        Optional<Config> maybeConfig = Optional.ofNullable(config);
        Set<String> apiKeyNames = maybeConfig.map(Config::apiKeyNames).orElse(Set.of());
        Level logLevel = maybeConfig.map(Config::logLevel).orElse(DEFAULT_LOG_LEVEL);
        String loggerName = maybeConfig.map(Config::loggerName).orElse(ProtocolLoggerFilter.class.getName());

        Set<String> resolvedNames = apiKeyNames.stream()
                .map(ProtocolLogger::resolveApiKeyName)
                .collect(Collectors.toUnmodifiableSet());

        return new Config(resolvedNames, logLevel, loggerName);
    }

    static String resolveApiKeyName(String input) {
        String normalized = normalizeKey(input);
        ApiKeys key = NORMALIZED_API_KEY_LOOKUP.get(normalized);
        if (key == null) {
            throw new PluginConfigurationException(
                    "Unknown API key name '" + input + "' in apiKeyNames. Must be a valid io.kroxylicious.kafka.common.protocol.ApiKeys name.");
        }
        return key.name();
    }

    private static String normalizeKey(String name) {
        return name.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "");
    }

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "config is marked @Nullable to match the @UnknownNullness contract of FilterFactory#createFilter,"
            + " but the runtime only invokes this method with the value returned by initialize, which is never null.")
    @Override
    public Filter createFilter(FilterFactoryContext context, @Nullable Config config) {
        Objects.requireNonNull(config);
        Set<ApiKeys> keys;
        if (config.apiKeyNames().isEmpty()) {
            keys = EnumSet.allOf(ApiKeys.class);
        }
        else {
            keys = config.apiKeyNames().stream()
                    .map(ApiKeys::valueOf)
                    .collect(Collectors.toCollection(() -> EnumSet.noneOf(ApiKeys.class)));
        }
        Logger logger = LoggerFactory.getLogger(config.loggerName());
        return new ProtocolLoggerFilter(keys, new MessageFormatter(), config.logLevel(), logger, new LogWarningThrottle(Clock.systemUTC(), config.loggerName()));
    }

}
