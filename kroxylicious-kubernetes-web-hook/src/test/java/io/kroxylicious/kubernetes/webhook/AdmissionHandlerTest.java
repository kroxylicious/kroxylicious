/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdmissionHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROXY_IMAGE = "quay.io/kroxylicious/kroxylicious:latest";

    @Mock
    private SidecarConfigResolver configResolver;

    private AdmissionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new AdmissionHandler(configResolver, PROXY_IMAGE);
    }

    @Test
    void allowsWhenRequestIsNull() {
        AdmissionReview review = new AdmissionReview();
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void allowsWhenPodObjectIsNull() {
        AdmissionReview review = reviewWithPod(null, "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void allowsAndPatchesWhenConfigExists() {
        KroxyliciousSidecarConfig config = sidecarConfig("test-ns", "config");
        when(configResolver.resolve("test-ns", null)).thenReturn(Optional.of(config));

        AdmissionReview review = reviewWithPod(minimalPod(), "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNotNull();
        assertThat(response.getPatchType()).isEqualTo("JSONPatch");
    }

    @Test
    void allowsWithoutPatchWhenNoConfig() {
        when(configResolver.resolve(eq("test-ns"), isNull())).thenReturn(Optional.empty());

        AdmissionReview review = reviewWithPod(minimalPod(), "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void allowsWithoutPatchWhenOptedOut() {
        Pod pod = minimalPod();
        pod.getMetadata().setAnnotations(
                new HashMap<>(Map.of(Annotations.INJECT_SIDECAR, "false")));

        // Config resolver should still be called, but injection is skipped
        when(configResolver.resolve(eq("test-ns"), isNull())).thenReturn(Optional.of(sidecarConfig("test-ns", "config")));

        AdmissionReview review = reviewWithPod(pod, "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void usesExplicitConfigName() {
        KroxyliciousSidecarConfig config = sidecarConfig("test-ns", "my-config");
        when(configResolver.resolve("test-ns", "my-config")).thenReturn(Optional.of(config));

        Pod pod = minimalPod();
        pod.getMetadata().setAnnotations(
                new HashMap<>(Map.of(Annotations.SIDECAR_CONFIG, "my-config")));

        AdmissionReview review = reviewWithPod(pod, "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNotNull();
    }

    @Test
    void preservesUidInResponse() {
        when(configResolver.resolve(any(), isNull())).thenReturn(Optional.empty());

        AdmissionReview review = reviewWithPod(minimalPod(), "test-ns");
        review.getRequest().setUid("test-uid-123");

        AdmissionResponse response = handler.processReview(review);
        assertThat(response.getUid()).isEqualTo("test-uid-123");
    }

    @Test
    void failsOpenOnException() {
        when(configResolver.resolve(any(), isNull())).thenThrow(new RuntimeException("kaboom"));

        AdmissionReview review = reviewWithPod(minimalPod(), "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void usesProxyImageFromConfig() {
        KroxyliciousSidecarConfig config = sidecarConfig("test-ns", "config");
        config.getSpec().setProxyImage("custom-image:v1");
        when(configResolver.resolve("test-ns", null)).thenReturn(Optional.of(config));

        AdmissionReview review = reviewWithPod(minimalPod(), "test-ns");
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNotNull();
        // The patch should contain the custom image — decode and verify
        String patchJson = new String(java.util.Base64.getDecoder().decode(response.getPatch()));
        assertThat(patchJson).contains("custom-image:v1");
    }

    private static AdmissionReview reviewWithPod(Pod pod, String namespace) {
        AdmissionReview review = new AdmissionReview();
        AdmissionRequest request = new AdmissionRequest();
        request.setUid("uid-" + System.nanoTime());
        request.setNamespace(namespace);
        if (pod != null) {
            request.setObject(MAPPER.valueToTree(pod));
        }
        review.setRequest(request);
        return review;
    }

    private static Pod minimalPod() {
        Pod pod = new Pod();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("test-pod");
        pod.setMetadata(meta);
        PodSpec spec = new PodSpec();
        spec.setContainers(new java.util.ArrayList<>());
        pod.setSpec(spec);
        return pod;
    }

    private static KroxyliciousSidecarConfig sidecarConfig(
                                                           String namespace,
                                                           String name) {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(namespace);
        meta.setName(name);
        config.setMetadata(meta);
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setUpstreamBootstrapServers("kafka.example.com:9092");
        config.setSpec(spec);
        return config;
    }
}
