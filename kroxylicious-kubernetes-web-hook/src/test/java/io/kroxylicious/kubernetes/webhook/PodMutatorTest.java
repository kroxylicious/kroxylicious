/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;

import static org.assertj.core.api.Assertions.assertThat;

class PodMutatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROXY_IMAGE = "quay.io/kroxylicious/kroxylicious:latest";

    @Test
    void patchSetsProxyConfigAnnotation() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        // Whether annotations exist or not, proxy-config annotation must be set
        String escapedKey = PodMutator.escapeJsonPointer(Annotations.PROXY_CONFIG);
        boolean hasIndividualAnnotation = !patchOps(patch, "add",
                "/metadata/annotations/" + escapedKey).isEmpty();
        boolean hasAnnotationsMap = !patchOps(patch, "add", "/metadata/annotations").isEmpty();
        assertThat(hasIndividualAnnotation || hasAnnotationsMap)
                .as("patch should set proxy-config annotation")
                .isTrue();
    }

    @Test
    void patchAddsSidecarStatusAnnotation() throws Exception {
        Pod pod = podWithAppContainer(Map.of("existing", "value"));
        JsonNode patch = createPatchJson(pod);

        String escapedKey = PodMutator.escapeJsonPointer(Annotations.SIDECAR_STATUS);
        assertThat(patchOps(patch, "add", "/metadata/annotations/" + escapedKey))
                .isNotEmpty();
    }

    @Test
    void patchAddsVolumeWhenNoVolumesExist() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        // With no existing volumes, the patch creates the volumes array
        assertThat(patchOps(patch, "add", "/spec/volumes"))
                .isNotEmpty();
    }

    @Test
    void patchAppendsVolumeWhenVolumesExist() throws Exception {
        Pod pod = podWithAppContainer(null);
        Volume existing = new Volume();
        existing.setName("existing-volume");
        pod.getSpec().setVolumes(new ArrayList<>(List.of(existing)));

        JsonNode patch = createPatchJson(pod);

        assertThat(patchOps(patch, "add", "/spec/volumes/-"))
                .isNotEmpty();
    }

    @Test
    void patchAppendsSidecarContainer() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        // Pod already has an app container, so sidecar is appended
        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        assertThat(containerOps).hasSize(1);

        JsonNode container = containerOps.get(0).path("value");
        assertThat(container.path("name").asText()).isEqualTo(InjectionDecision.SIDECAR_CONTAINER_NAME);
        assertThat(container.path("image").asText()).isEqualTo(PROXY_IMAGE);
    }

    @Test
    void sidecarContainerHasSecurityContext() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode secCtx = containerOps.get(0).path("value").path("securityContext");
        assertThat(secCtx.path("allowPrivilegeEscalation").asBoolean()).isFalse();
        assertThat(secCtx.path("readOnlyRootFilesystem").asBoolean()).isTrue();
        assertThat(secCtx.path("capabilities").path("drop").get(0).asText()).isEqualTo("ALL");
    }

    @Test
    void sidecarContainerHasProbes() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode container = containerOps.get(0).path("value");
        assertThat(container.has("startupProbe")).isTrue();
        assertThat(container.has("livenessProbe")).isTrue();
        assertThat(container.has("readinessProbe")).isTrue();
        assertThat(container.path("startupProbe").path("httpGet").path("path").asText())
                .isEqualTo("/livez");
    }

    @Test
    void sidecarContainerHasVolumeMount() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode mounts = containerOps.get(0).path("value").path("volumeMounts");
        assertThat(mounts).hasSize(1);
        assertThat(mounts.get(0).path("name").asText()).isEqualTo("kroxylicious-config");
        assertThat(mounts.get(0).path("readOnly").asBoolean()).isTrue();
    }

    @Test
    void patchSetsBootstrapEnvVar() throws Exception {
        Pod pod = podWithAppContainer(null);

        JsonNode patch = createPatchJson(pod);

        // App container at index 0 should get env var
        boolean hasEnvArray = !patchOps(patch, "add", "/spec/containers/0/env").isEmpty();
        boolean hasEnvAppend = !patchOps(patch, "add", "/spec/containers/0/env/-").isEmpty();
        assertThat(hasEnvArray || hasEnvAppend)
                .as("patch should set KAFKA_BOOTSTRAP_SERVERS on app container")
                .isTrue();
    }

    @Test
    void patchAppendsBootstrapEnvVarWhenEnvExists() throws Exception {
        Pod pod = podWithAppContainer(null);
        Container app = pod.getSpec().getContainers().get(0);
        EnvVar existingEnv = new EnvVar();
        existingEnv.setName("OTHER_VAR");
        existingEnv.setValue("other");
        app.setEnv(new ArrayList<>(List.of(existingEnv)));

        JsonNode patch = createPatchJson(pod);

        assertThat(patchOps(patch, "add", "/spec/containers/0/env/-"))
                .isNotEmpty();
    }

    @Test
    void patchReplacesExistingBootstrapEnvVar() throws Exception {
        Pod pod = podWithAppContainer(null);
        Container app = pod.getSpec().getContainers().get(0);
        EnvVar bootstrapEnv = new EnvVar();
        bootstrapEnv.setName("KAFKA_BOOTSTRAP_SERVERS");
        bootstrapEnv.setValue("old-kafka:9092");
        app.setEnv(new ArrayList<>(List.of(bootstrapEnv)));

        JsonNode patch = createPatchJson(pod);

        List<JsonNode> replaceOps = patchOps(patch, "replace", "/spec/containers/0/env/0/value");
        assertThat(replaceOps).hasSize(1);
        assertThat(replaceOps.get(0).path("value").asText()).isEqualTo("localhost:19092");
    }

    @Test
    void patchSkipsBootstrapEnvVarWhenDisabled() throws Exception {
        Pod pod = podWithAppContainer(null);

        KroxyliciousSidecarConfigSpec spec = defaultSpec();
        spec.setSetBootstrapEnvVar(false);

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);

        assertThat(patchOps(patch, "add", "/spec/containers/0/env")).isEmpty();
        assertThat(patchOps(patch, "add", "/spec/containers/0/env/-")).isEmpty();
    }

    @Test
    void patchAddsPodSecurityContextWhenAbsent() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        List<JsonNode> secCtxOps = patchOps(patch, "add", "/spec/securityContext");
        assertThat(secCtxOps).hasSize(1);
        assertThat(secCtxOps.get(0).path("value").path("runAsNonRoot").asBoolean()).isTrue();
    }

    @Test
    void patchPreservesExistingPodSecurityContext() throws Exception {
        Pod pod = podWithAppContainer(null);
        PodSecurityContext secCtx = new PodSecurityContext();
        secCtx.setRunAsNonRoot(true);
        pod.getSpec().setSecurityContext(secCtx);

        JsonNode patch = createPatchJson(pod);

        assertThat(patchOps(patch, "add", "/spec/securityContext")).isEmpty();
    }

    @Test
    void escapeJsonPointerHandsTildeAndSlash() {
        assertThat(PodMutator.escapeJsonPointer("a~b/c")).isEqualTo("a~0b~1c");
    }

    @Test
    void escapeJsonPointerHandlesPlainString() {
        assertThat(PodMutator.escapeJsonPointer("simple.key")).isEqualTo("simple.key");
    }

    @Test
    void patchIsValidJsonArray() throws Exception {
        Pod pod = podWithAppContainer(null);
        String patchStr = PodMutator.createPatch(pod, defaultSpec(), PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);
        assertThat(patch.isArray()).isTrue();
        assertThat(patch).isNotEmpty();
    }

    /**
     * Creates a pod with one application container already present.
     * This ensures PodMutator uses the append ({@code /-}) path for adding the sidecar.
     */
    private static Pod podWithAppContainer(Map<String, String> annotations) {
        Pod pod = new Pod();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("test-pod");
        meta.setNamespace("default");
        if (annotations != null) {
            meta.setAnnotations(new HashMap<>(annotations));
        }
        pod.setMetadata(meta);

        PodSpec spec = new PodSpec();
        Container app = new Container();
        app.setName("my-app");
        app.setImage("my-app:latest");
        spec.setContainers(new ArrayList<>(List.of(app)));
        pod.setSpec(spec);
        return pod;
    }

    private static KroxyliciousSidecarConfigSpec defaultSpec() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setUpstreamBootstrapServers("kafka.example.com:9092");
        return spec;
    }

    private JsonNode createPatchJson(Pod pod) throws Exception {
        String patchStr = PodMutator.createPatch(pod, defaultSpec(), PROXY_IMAGE);
        return MAPPER.readTree(patchStr);
    }

    private static List<JsonNode> patchOps(
                                           JsonNode patch,
                                           String op,
                                           String path) {
        List<JsonNode> result = new ArrayList<>();
        for (JsonNode node : patch) {
            if (op.equals(node.path("op").asText()) && path.equals(node.path("path").asText())) {
                result.add(node);
            }
        }
        return result;
    }
}
