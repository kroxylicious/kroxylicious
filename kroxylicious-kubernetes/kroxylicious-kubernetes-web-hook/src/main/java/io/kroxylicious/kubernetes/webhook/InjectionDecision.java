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
        INJECT,
        SKIP_ALREADY_INJECTED,
        SKIP_OPT_OUT,
        SKIP_NO_CONFIG
    }

    private InjectionDecision() {
    }

    /**
     * Evaluates whether the given pod should have a sidecar injected.
     *
     * @param pod the pod being admitted
     * @param hasConfig whether a {@code KroxyliciousSidecarConfig} was found for the pod's namespace
     * @return the injection decision
     */
    @NonNull
    static Decision evaluate(
                             @NonNull Pod pod,
                             boolean hasConfig) {

        if (isOptedOut(pod)) {
            return Decision.SKIP_OPT_OUT;
        }

        if (isAlreadyInjected(pod)) {
            return Decision.SKIP_ALREADY_INJECTED;
        }

        if (!hasConfig) {
            return Decision.SKIP_NO_CONFIG;
        }

        return Decision.INJECT;
    }

    private static boolean isOptedOut(@NonNull Pod pod) {
        Map<String, String> annotations = annotations(pod);
        String value = annotations.get(Annotations.INJECT_SIDECAR);
        return "false".equals(value);
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
    private static Map<String, String> annotations(@NonNull Pod pod) {
        @Nullable
        Map<String, String> annotations = pod.getMetadata() != null ? pod.getMetadata().getAnnotations() : null;
        return annotations != null ? annotations : Map.of();
    }
}
