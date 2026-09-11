/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.config.ControllerConfigurationOverrider;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configures JOSDK controller reconciliation settings including watched namespaces
 * and maximum reconciliation interval.
 */
class ControllerConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerConfigurer.class);
    private static final Duration DEFAULT_MAX_RECONCILIATION_INTERVAL = Duration.ofMinutes(3);
    private static final Pattern WATCHED_NAMESPACE_SPLITTER = Pattern.compile(" *, *");

    private final Duration maxReconciliationInterval;
    @Nullable
    private final Set<String> watchedNamespaces;

    ControllerConfigurer() {
        this(getWatchedNamespacesFromEnvironment(), getMaxReconciliationIntervalFromEnvironment());
    }

    @VisibleForTesting
    ControllerConfigurer(@Nullable Set<String> watchedNamespaces, Duration maxReconciliationInterval) {
        this.watchedNamespaces = watchedNamespaces;
        this.maxReconciliationInterval = maxReconciliationInterval;
        logConfiguration();
    }

    private void logConfiguration() {
        LOGGER.atInfo()
                .addKeyValue("interval", maxReconciliationInterval)
                .log("Configuring operator max reconciliation interval");

        Optional.ofNullable(watchedNamespaces)
                .ifPresentOrElse(
                        ns -> LOGGER.atInfo().addKeyValue("namespaces", ns).log("Watching namespaces"),
                        () -> LOGGER.atInfo().log("Watching all namespaces"));
    }

    @NonNull
    <T extends HasMetadata> Consumer<ControllerConfigurationOverrider<T>> configurationOverrider() {
        return configOverrider -> {
            configOverrider.withReconciliationMaxInterval(maxReconciliationInterval);
            Optional.ofNullable(watchedNamespaces)
                    .filter(Predicate.not(Set::isEmpty))
                    .ifPresent(configOverrider::settingNamespaces);
        };
    }

    @Nullable
    Set<String> getWatchedNamespaces() {
        return watchedNamespaces;
    }

    @NonNull
    private static Duration getMaxReconciliationIntervalFromEnvironment() {
        String envValue = System.getenv(OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME);
        if (envValue != null) {
            try {
                long seconds = Long.parseLong(envValue);
                if (seconds <= 0) {
                    LOGGER.atWarn()
                            .addKeyValue("envVar", OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
                            .addKeyValue("value", envValue)
                            .addKeyValue("default", DEFAULT_MAX_RECONCILIATION_INTERVAL)
                            .log("Invalid value (must be positive), using default");
                    return DEFAULT_MAX_RECONCILIATION_INTERVAL;
                }
                return Duration.ofSeconds(seconds);
            }
            catch (NumberFormatException e) {
                LOGGER.atWarn()
                        .addKeyValue("envVar", OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
                        .addKeyValue("value", envValue)
                        .addKeyValue("default", DEFAULT_MAX_RECONCILIATION_INTERVAL)
                        .log("Invalid value (not a number), using default");
            }
        }
        return DEFAULT_MAX_RECONCILIATION_INTERVAL;
    }

    @Nullable
    private static Set<String> getWatchedNamespacesFromEnvironment() {
        var targets = Optional.ofNullable(System.getenv().get(OperatorMain.KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty));

        if (targets.isEmpty()) {
            return null;
        }

        return targets.stream()
                .flatMap(WATCHED_NAMESPACE_SPLITTER::splitAsStream)
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .collect(Collectors.toSet());
    }
}
