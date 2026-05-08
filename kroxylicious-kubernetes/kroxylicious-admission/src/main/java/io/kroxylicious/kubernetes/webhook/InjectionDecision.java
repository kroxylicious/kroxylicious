/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.Map;

import io.fabric8.kubernetes.api.model.Pod;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Determines whether a sidecar should be injected into a pod.
 */
final class InjectionDecision {

    static final String SIDECAR_CONTAINER_NAME = "kroxylicious-proxy";

    enum Decision {
        INJECT(null),
        SKIP_ALREADY_INJECTED("container-name-conflict"),
        SKIP_OPT_OUT(null),
        SKIP_NO_CONFIG("no-KroxyliciousSidecarConfig"),
        SKIP_MULTIPLE_CONFIGS("ambiguous-KroxyliciousSidecarConfig");

        private final String skipLabel;

        Decision(String skipLabel) {
            this.skipLabel = skipLabel;
        }

        /**
         * @return the value for the {@code sidecar.kroxylicious.io/injection-skipped}
         *         label, or {@code null} if this decision should not label the pod.
         */
        String skipLabel() {
            return skipLabel;
        }

        boolean isConfigUnavailable() {
            return this == SKIP_NO_CONFIG || this == SKIP_MULTIPLE_CONFIGS;
        }
    }

    private InjectionDecision() {
    }

    /**
     * Evaluates whether the given pod should have a sidecar injected.
     *
     * @param pod the pod being admitted
     * @param resolveOutcome the outcome of resolving the {@code KroxyliciousSidecarConfig}
     * @return the injection decision
     */
    @NonNull
    static Decision evaluate(
                             @NonNull Pod pod,
                             @NonNull SidecarConfigResolver.Resolution.Outcome resolveOutcome) {

        if (isOptedOut(pod)) {
            return Decision.SKIP_OPT_OUT;
        }

        if (isAlreadyInjected(pod)) {
            return Decision.SKIP_ALREADY_INJECTED;
        }

        return switch (resolveOutcome) {
            case FOUND -> Decision.INJECT;
            case NO_CONFIG -> Decision.SKIP_NO_CONFIG;
            case MULTIPLE_CONFIGS -> Decision.SKIP_MULTIPLE_CONFIGS;
        };
    }

    private static boolean isOptedOut(@NonNull Pod pod) {
        Map<String, String> labels = labels(pod);
        String value = labels.get(Labels.SIDECAR_INJECTION);
        return Labels.SIDECAR_INJECTION_DISABLED.equals(value);
    }

    private static boolean isAlreadyInjected(@NonNull Pod pod) {
        if (pod.getSpec() == null) {
            return false;
        }
        if (pod.getSpec().getContainers() != null
                && pod.getSpec().getContainers().stream()
                        .anyMatch(c -> SIDECAR_CONTAINER_NAME.equals(c.getName()))) {
            return true;
        }
        // Also check initContainers for native sidecar mode (K8s 1.28+)
        return pod.getSpec().getInitContainers() != null
                && pod.getSpec().getInitContainers().stream()
                        .anyMatch(c -> SIDECAR_CONTAINER_NAME.equals(c.getName()));
    }

    @NonNull
    private static Map<String, String> labels(@NonNull Pod pod) {
        @Nullable
        Map<String, String> labels = pod.getMetadata() != null ? pod.getMetadata().getLabels() : null;
        return labels != null ? labels : Map.of();
    }
}
