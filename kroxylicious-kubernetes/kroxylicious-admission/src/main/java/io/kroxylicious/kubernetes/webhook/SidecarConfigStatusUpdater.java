/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.time.Clock;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigStatus;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Updates the status of {@link KroxyliciousSidecarConfig} resources
 * to signal that the webhook has observed and accepted the configuration.
 */
class SidecarConfigStatusUpdater {

    static final String REASON_ACCEPTED = "Accepted";

    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarConfigStatusUpdater.class);

    private final KubernetesClient client;
    private final Clock clock;

    SidecarConfigStatusUpdater(
                               @NonNull KubernetesClient client,
                               @NonNull Clock clock) {
        this.client = client;
        this.clock = clock;
    }

    /**
     * Sets the {@code Ready=True} condition on the given config's status,
     * unless it is already set for the current generation.
     */
    void setReady(@NonNull KroxyliciousSidecarConfig config) {
        if (isAlreadyReady(config)) {
            return;
        }
        String namespace = config.getMetadata().getNamespace();
        String name = config.getMetadata().getName();
        try {
            long generation = generation(config);
            Condition readyCondition = new ConditionBuilder()
                    .withType(Condition.Type.Ready)
                    .withStatus(Condition.Status.TRUE)
                    .withReason(REASON_ACCEPTED)
                    .withMessage("")
                    .withLastTransitionTime(clock.instant())
                    .withObservedGeneration(generation)
                    .build();

            client.resources(KroxyliciousSidecarConfig.class)
                    .inNamespace(namespace)
                    .withName(name)
                    .editStatus(existing -> {
                        KroxyliciousSidecarConfigStatus status = existing.getStatus();
                        if (status == null) {
                            status = new KroxyliciousSidecarConfigStatus();
                            existing.setStatus(status);
                        }
                        status.setConditions(List.of(readyCondition));
                        status.setObservedGeneration(generation);
                        return existing;
                    });

            LOGGER.atDebug()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                    .addKeyValue(WebhookLoggingKeys.NAME, name)
                    .log("Set Ready condition on KroxyliciousSidecarConfig");
        }
        catch (Exception e) {
            LOGGER.atWarn()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                    .addKeyValue(WebhookLoggingKeys.NAME, name)
                    .addKeyValue("error", e.getMessage())
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "Failed to update KroxyliciousSidecarConfig status"
                            : "Failed to update KroxyliciousSidecarConfig status, increase log level to DEBUG for stacktrace");
        }
    }

    @VisibleForTesting
    static boolean isAlreadyReady(@NonNull KroxyliciousSidecarConfig config) {
        KroxyliciousSidecarConfigStatus status = config.getStatus();
        if (status == null || status.getConditions() == null) {
            return false;
        }
        long generation = generation(config);
        return status.getConditions().stream()
                .anyMatch(c -> Condition.Type.Ready.equals(c.getType())
                        && Condition.Status.TRUE.equals(c.getStatus())
                        && c.getObservedGeneration() != null
                        && c.getObservedGeneration() >= generation);
    }

    private static long generation(@NonNull KroxyliciousSidecarConfig config) {
        Long gen = config.getMetadata().getGeneration();
        return gen != null ? gen : 0L;
    }
}
