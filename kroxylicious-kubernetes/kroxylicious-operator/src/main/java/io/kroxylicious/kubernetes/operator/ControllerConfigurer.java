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
 * and, optionally, the maximum reconciliation interval.
 * <p>
 * By default the JOSDK maximum reconciliation interval is left untouched, so the operator relies
 * on event-driven reconciliation. Setting {@link OperatorMain#KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME}
 * overrides the interval and acts as an escape hatch should periodic reconciliation ever be needed again.
 * </p>
 */
class ControllerConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerConfigurer.class);
    private static final Pattern WATCHED_NAMESPACE_SPLITTER = Pattern.compile(" *, *");

    @Nullable
    private final Duration maxReconciliationInterval;
    @Nullable
    private final Set<String> watchedNamespaces;

    ControllerConfigurer() {
        this(getWatchedNamespacesFromEnvironment(), getMaxReconciliationIntervalFromEnvironment());
    }

    /**
     * @param watchedNamespaces namespaces to watch, or {@code null} to watch all namespaces
     * @param maxReconciliationInterval maximum reconciliation interval to configure on every controller,
     *                                  or {@code null} to leave the JOSDK default in place
     */
    @VisibleForTesting
    ControllerConfigurer(@Nullable Set<String> watchedNamespaces, @Nullable Duration maxReconciliationInterval) {
        this.watchedNamespaces = watchedNamespaces;
        this.maxReconciliationInterval = maxReconciliationInterval;
        logConfiguration();
    }

    private void logConfiguration() {
        Optional.ofNullable(maxReconciliationInterval)
                .ifPresentOrElse(
                        interval -> LOGGER.atInfo().addKeyValue("interval", interval).log("Configuring operator max reconciliation interval"),
                        () -> LOGGER.atInfo().log("Using JOSDK default max reconciliation interval"));

        Optional.ofNullable(watchedNamespaces)
                .ifPresentOrElse(
                        ns -> LOGGER.atInfo().addKeyValue("namespaces", ns).log("Watching namespaces"),
                        () -> LOGGER.atInfo().log("Watching all namespaces"));
    }

    @NonNull
    <T extends HasMetadata> Consumer<ControllerConfigurationOverrider<T>> configurationOverrider() {
        return configOverrider -> {
            Optional.ofNullable(maxReconciliationInterval)
                    .ifPresent(configOverrider::withReconciliationMaxInterval);
            Optional.ofNullable(watchedNamespaces)
                    .filter(Predicate.not(Set::isEmpty))
                    .ifPresent(configOverrider::settingNamespaces);
        };
    }

    @Nullable
    Set<String> getWatchedNamespaces() {
        return watchedNamespaces;
    }

    /**
     * @return the configured maximum reconciliation interval, or empty if the JOSDK default is used
     */
    @NonNull
    Optional<Duration> getMaxReconciliationInterval() {
        return Optional.ofNullable(maxReconciliationInterval);
    }

    @Nullable
    private static Duration getMaxReconciliationIntervalFromEnvironment() {
        String envValue = System.getenv(OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME);
        if (envValue == null) {
            return null;
        }
        try {
            long seconds = Long.parseLong(envValue);
            if (seconds <= 0) {
                LOGGER.atWarn()
                        .addKeyValue("envVar", OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
                        .addKeyValue("value", envValue)
                        .log("Invalid value (must be positive), using JOSDK default max reconciliation interval");
                return null;
            }
            return Duration.ofSeconds(seconds);
        }
        catch (NumberFormatException e) {
            LOGGER.atWarn()
                    .addKeyValue("envVar", OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
                    .addKeyValue("value", envValue)
                    .log("Invalid value (not a number), using JOSDK default max reconciliation interval");
            return null;
        }
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
