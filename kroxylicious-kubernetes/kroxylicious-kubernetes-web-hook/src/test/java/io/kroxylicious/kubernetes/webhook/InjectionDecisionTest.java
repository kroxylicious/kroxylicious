/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;

import static org.assertj.core.api.Assertions.assertThat;

class InjectionDecisionTest {

    @Test
    void shouldInjectWhenConfigAvailableAndNotOptedOut() {
        Pod pod = podWithAnnotations(Map.of());
        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.INJECT);
    }

    @Test
    void shouldInjectWhenNoAnnotations() {
        Pod pod = new Pod();
        pod.setMetadata(new ObjectMeta());
        pod.setSpec(new PodSpec());
        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.INJECT);
    }

    @Test
    void shouldSkipWhenOptedOut() {
        Pod pod = podWithAnnotations(Map.of(Annotations.INJECT_SIDECAR, "false"));
        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.SKIP_OPT_OUT);
    }

    @Test
    void shouldInjectWhenAnnotationIsNotFalse() {
        Pod pod = podWithAnnotations(Map.of(Annotations.INJECT_SIDECAR, "true"));
        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.INJECT);
    }

    @Test
    void shouldSkipWhenAlreadyInjected() {
        Pod pod = podWithAnnotations(Map.of());
        Container sidecar = new Container();
        sidecar.setName(InjectionDecision.SIDECAR_CONTAINER_NAME);
        pod.getSpec().getContainers().add(sidecar);

        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.SKIP_ALREADY_INJECTED);
    }

    @Test
    void shouldSkipWhenNoConfig() {
        Pod pod = podWithAnnotations(Map.of());
        assertThat(InjectionDecision.evaluate(pod, false)).isEqualTo(InjectionDecision.Decision.SKIP_NO_CONFIG);
    }

    @Test
    void optOutTakesPriorityOverAlreadyInjected() {
        Pod pod = podWithAnnotations(Map.of(Annotations.INJECT_SIDECAR, "false"));
        Container sidecar = new Container();
        sidecar.setName(InjectionDecision.SIDECAR_CONTAINER_NAME);
        pod.getSpec().getContainers().add(sidecar);

        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.SKIP_OPT_OUT);
    }

    @Test
    void alreadyInjectedTakesPriorityOverNoConfig() {
        Pod pod = podWithAnnotations(Map.of());
        Container sidecar = new Container();
        sidecar.setName(InjectionDecision.SIDECAR_CONTAINER_NAME);
        pod.getSpec().getContainers().add(sidecar);

        assertThat(InjectionDecision.evaluate(pod, false)).isEqualTo(InjectionDecision.Decision.SKIP_ALREADY_INJECTED);
    }

    @Test
    void shouldSkipWhenAlreadyInjectedAsInitContainer() {
        Pod pod = podWithAnnotations(Map.of());
        Container sidecar = new Container();
        sidecar.setName(InjectionDecision.SIDECAR_CONTAINER_NAME);
        pod.getSpec().setInitContainers(new java.util.ArrayList<>(java.util.List.of(sidecar)));

        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.SKIP_ALREADY_INJECTED);
    }

    @Test
    void shouldHandleNullSpec() {
        Pod pod = new Pod();
        pod.setMetadata(new ObjectMeta());
        assertThat(InjectionDecision.evaluate(pod, true)).isEqualTo(InjectionDecision.Decision.INJECT);
    }

    private static Pod podWithAnnotations(Map<String, String> annotations) {
        Pod pod = new Pod();
        ObjectMeta meta = new ObjectMeta();
        meta.setAnnotations(new java.util.HashMap<>(annotations));
        pod.setMetadata(meta);
        PodSpec spec = new PodSpec();
        spec.setContainers(new java.util.ArrayList<>());
        pod.setSpec(spec);
        return pod;
    }
}
