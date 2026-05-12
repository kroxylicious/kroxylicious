/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;

import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.VirtualClusters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdmissionHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROXY_IMAGE = "quay.io/kroxylicious/kroxylicious:latest";
    public static final String TEST_NS = "test-ns";

    @Mock
    private SidecarConfigResolver configResolver;

    private AdmissionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new AdmissionHandler(configResolver, PROXY_IMAGE);
    }

    // --- processReview() tests ---

    @Test
    void allowsWhenRequestIsNull() {
        AdmissionReview review = new AdmissionReview();
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void allowsWhenPodObjectIsNull() {
        AdmissionReview review = reviewWithPod(null);
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void allowsAndPatchesWhenConfigExists() {
        KroxyliciousSidecarConfig config = sidecarConfig("config");
        when(configResolver.resolve(TEST_NS, null)).thenReturn(SidecarConfigResolver.Resolution.found(config));

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNotNull();
        assertThat(response.getPatchType()).isEqualTo("JSONPatch");
    }

    @Test
    void labelsSkippedPodWhenNoConfig() {
        when(configResolver.resolve(eq(TEST_NS), isNull())).thenReturn(SidecarConfigResolver.Resolution.noConfig());

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatchType()).isEqualTo("JSONPatch");
        String patchJson = new String(java.util.Base64.getDecoder().decode(response.getPatch()));
        assertThat(patchJson).contains(Labels.INJECTION_SKIPPED)
                .contains("no-KroxyliciousSidecarConfig");
    }

    @Test
    void allowsWithoutPatchWhenOptedOut() {
        Pod pod = minimalPod();
        pod.getMetadata().setLabels(
                new HashMap<>(Map.of(Labels.SIDECAR_INJECTION, Labels.SIDECAR_INJECTION_DISABLED)));

        // Config resolver should still be called, but injection is skipped
        when(configResolver.resolve(eq(TEST_NS), isNull()))
                .thenReturn(SidecarConfigResolver.Resolution.found(sidecarConfig("config")));

        AdmissionReview review = reviewWithPod(pod);
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNull();
    }

    @Test
    void labelsSkippedPodWhenAlreadyInjected() {
        when(configResolver.resolve(eq(TEST_NS), isNull()))
                .thenReturn(SidecarConfigResolver.Resolution.found(sidecarConfig("config")));

        Pod pod = minimalPod();
        io.fabric8.kubernetes.api.model.Container sidecar = new io.fabric8.kubernetes.api.model.Container();
        sidecar.setName(InjectionDecision.SIDECAR_CONTAINER_NAME);
        pod.getSpec().getContainers().add(sidecar);

        AdmissionReview review = reviewWithPod(pod);
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatchType()).isEqualTo("JSONPatch");
        String patchJson = new String(java.util.Base64.getDecoder().decode(response.getPatch()));
        assertThat(patchJson).contains(Labels.INJECTION_SKIPPED)
                .contains("container-name-conflict");
    }

    @Test
    void labelsSkippedPodWhenMultipleConfigs() {
        when(configResolver.resolve(eq(TEST_NS), isNull())).thenReturn(SidecarConfigResolver.Resolution.multipleConfigs());

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatchType()).isEqualTo("JSONPatch");
        String patchJson = new String(java.util.Base64.getDecoder().decode(response.getPatch()));
        assertThat(patchJson).contains(Labels.INJECTION_SKIPPED)
                .contains("ambiguous-KroxyliciousSidecarConfig");
    }

    @Test
    void labelsSkippedPodWhenConfigIsInvalid() {
        when(configResolver.resolve(eq(TEST_NS), isNull())).thenReturn(SidecarConfigResolver.Resolution.invalidConfig());

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatchType()).isEqualTo("JSONPatch");
        String patchJson = new String(java.util.Base64.getDecoder().decode(response.getPatch()));
        assertThat(patchJson).contains(Labels.INJECTION_SKIPPED)
                .contains("invalid-KroxyliciousSidecarConfig");
    }

    @Test
    void deniesWhenConfigIsInvalidAndPolicyIsDeny() {
        var denyHandler = new AdmissionHandler(
                configResolver, PROXY_IMAGE, new KubernetesVersion(1, 0), true);
        when(configResolver.resolve(eq(TEST_NS), isNull()))
                .thenReturn(SidecarConfigResolver.Resolution.invalidConfig());

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = denyHandler.processReview(review);

        assertThat(response.getAllowed()).isFalse();
        assertThat(response.getStatus().getCode()).isEqualTo(403);
        assertThat(response.getStatus().getMessage()).contains("SKIP_INVALID_CONFIG");
    }

    @Test
    void deniesWhenNoConfigAndPolicyIsDeny() {
        var denyHandler = new AdmissionHandler(
                configResolver, PROXY_IMAGE, new KubernetesVersion(1, 0), true);
        when(configResolver.resolve(eq(TEST_NS), isNull()))
                .thenReturn(SidecarConfigResolver.Resolution.noConfig());

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = denyHandler.processReview(review);

        assertThat(response.getAllowed()).isFalse();
        assertThat(response.getStatus().getCode()).isEqualTo(403);
        assertThat(response.getStatus().getMessage()).contains("SKIP_NO_CONFIG");
    }

    @Test
    void deniesWhenMultipleConfigsAndPolicyIsDeny() {
        var denyHandler = new AdmissionHandler(
                configResolver, PROXY_IMAGE, new KubernetesVersion(1, 0), true);
        when(configResolver.resolve(eq(TEST_NS), isNull()))
                .thenReturn(SidecarConfigResolver.Resolution.multipleConfigs());

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = denyHandler.processReview(review);

        assertThat(response.getAllowed()).isFalse();
        assertThat(response.getStatus().getCode()).isEqualTo(403);
        assertThat(response.getStatus().getMessage()).contains("SKIP_MULTIPLE_CONFIGS");
    }

    @Test
    void allowsOptOutEvenWhenPolicyIsDeny() {
        var denyHandler = new AdmissionHandler(
                configResolver, PROXY_IMAGE, new KubernetesVersion(1, 0), true);
        when(configResolver.resolve(eq(TEST_NS), isNull()))
                .thenReturn(SidecarConfigResolver.Resolution.found(sidecarConfig("config")));

        Pod pod = minimalPod();
        pod.getMetadata().setLabels(
                new HashMap<>(Map.of(Labels.SIDECAR_INJECTION, Labels.SIDECAR_INJECTION_DISABLED)));

        AdmissionReview review = reviewWithPod(pod);
        AdmissionResponse response = denyHandler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
    }

    @Test
    void usesExplicitConfigName() {
        KroxyliciousSidecarConfig config = sidecarConfig("my-config");
        when(configResolver.resolve(TEST_NS, "my-config")).thenReturn(SidecarConfigResolver.Resolution.found(config));

        Pod pod = minimalPod();
        pod.getMetadata().setAnnotations(
                new HashMap<>(Map.of(Annotations.SIDECAR_CONFIG, "my-config")));

        AdmissionReview review = reviewWithPod(pod);
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNotNull();
    }

    @Test
    void preservesUidInResponse() {
        when(configResolver.resolve(any(), isNull())).thenReturn(SidecarConfigResolver.Resolution.noConfig());

        AdmissionReview review = reviewWithPod(minimalPod());
        review.getRequest().setUid("test-uid-123");

        AdmissionResponse response = handler.processReview(review);
        assertThat(response.getUid()).isEqualTo("test-uid-123");
    }

    @Test
    void propagatesExceptionFromProcessReview() {
        when(configResolver.resolve(any(), isNull())).thenThrow(new RuntimeException("kaboom"));

        AdmissionReview review = reviewWithPod(minimalPod());

        assertThatThrownBy(() -> handler.processReview(review))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("kaboom");
    }

    @Test
    void usesProxyImageFromConfig() {
        KroxyliciousSidecarConfig config = sidecarConfig("config");
        config.getSpec().setProxyImage("custom-image:v1");
        when(configResolver.resolve(TEST_NS, null)).thenReturn(SidecarConfigResolver.Resolution.found(config));

        AdmissionReview review = reviewWithPod(minimalPod());
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getPatch()).isNotNull();
        // The patch should contain the custom image — decode and verify
        String patchJson = new String(java.util.Base64.getDecoder().decode(response.getPatch()));
        assertThat(patchJson).contains("custom-image:v1");
    }

    @Test
    void processReviewHandlesPodWithGenerateNameButNoName() {
        when(configResolver.resolve(eq(TEST_NS), isNull())).thenReturn(SidecarConfigResolver.Resolution.noConfig());

        Pod pod = new Pod();
        ObjectMeta meta = new ObjectMeta();
        meta.setGenerateName("my-deployment-");
        pod.setMetadata(meta);
        PodSpec spec = new PodSpec();
        spec.setContainers(new java.util.ArrayList<>());
        pod.setSpec(spec);

        AdmissionReview review = reviewWithPod(pod);
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
    }

    @Test
    void processReviewHandlesPodWithNullMetadata() {
        when(configResolver.resolve(eq(TEST_NS), isNull())).thenReturn(SidecarConfigResolver.Resolution.noConfig());

        Pod pod = new Pod();

        AdmissionReview review = reviewWithPod(pod);
        AdmissionResponse response = handler.processReview(review);

        assertThat(response.getAllowed()).isTrue();
    }

    // --- handle(HttpExchange) tests ---

    @ParameterizedTest
    @ValueSource(strings = { "GET", "PUT", "DELETE", "PATCH" })
    void handleRejectsNonPostMethods(String method) throws IOException {
        HttpExchange exchange = createMockExchange(method, new byte[0]);

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(405, -1);
    }

    @Test
    void handlePostReturnsAdmissionReviewResponse() throws IOException {
        when(configResolver.resolve(eq(TEST_NS), isNull())).thenReturn(SidecarConfigResolver.Resolution.noConfig());

        AdmissionReview review = reviewWithPod(minimalPod());
        byte[] requestBody = MAPPER.writeValueAsBytes(review);
        HttpExchange exchange = createMockExchange("POST", requestBody);

        handler.handle(exchange);

        ByteArrayOutputStream responseBody = (ByteArrayOutputStream) exchange.getResponseBody();
        JsonNode responseJson = MAPPER.readTree(responseBody.toByteArray());

        assertThat(responseJson.get("apiVersion").asText()).isEqualTo("admission.k8s.io/v1");
        assertThat(responseJson.get("kind").asText()).isEqualTo("AdmissionReview");
        assertThat(responseJson.get("response").get("allowed").asBoolean()).isTrue();
        assertThat(exchange.getResponseHeaders().getFirst("Content-Type"))
                .isEqualTo("application/json");
    }

    @Test
    void handleReturns500OnDeserialisationError() throws IOException {
        HttpExchange exchange = createMockExchange("POST", "not json".getBytes(StandardCharsets.UTF_8));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(500, -1L);
    }

    @Test
    void handleReturns500OnProcessReviewException() throws IOException {
        when(configResolver.resolve(any(), isNull())).thenThrow(new RuntimeException("kaboom"));

        AdmissionReview review = reviewWithPod(minimalPod());
        byte[] requestBody = MAPPER.writeValueAsBytes(review);
        HttpExchange exchange = createMockExchange("POST", requestBody);

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(500, -1L);
    }

    // --- helpers ---

    private static HttpExchange createMockExchange(
                                                   String method,
                                                   byte[] body) {
        HttpExchange exchange = org.mockito.Mockito.mock(HttpExchange.class);
        org.mockito.Mockito.lenient().when(exchange.getRequestMethod()).thenReturn(method);
        org.mockito.Mockito.lenient().when(exchange.getRequestBody()).thenReturn(new ByteArrayInputStream(body));
        ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
        org.mockito.Mockito.lenient().when(exchange.getResponseBody()).thenReturn(responseBody);
        Headers headers = new Headers();
        org.mockito.Mockito.lenient().when(exchange.getResponseHeaders()).thenReturn(headers);
        return exchange;
    }

    private static AdmissionReview reviewWithPod(Pod pod) {
        AdmissionReview review = new AdmissionReview();
        AdmissionRequest request = new AdmissionRequest();
        request.setUid("uid-" + System.nanoTime());
        request.setNamespace(TEST_NS);
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
                                                           String name) {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(TEST_NS);
        meta.setName(name);
        config.setMetadata(meta);
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        VirtualClusters vc = new VirtualClusters();
        vc.setName("sidecar");
        vc.setTargetBootstrapServers("kafka.example.com:9092");
        spec.setVirtualClusters(List.of(vc));
        config.setSpec(spec);
        return config;
    }
}
