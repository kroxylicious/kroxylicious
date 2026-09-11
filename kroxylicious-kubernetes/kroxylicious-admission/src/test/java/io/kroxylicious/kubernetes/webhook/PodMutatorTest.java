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
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;

import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.Plugins;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.SecretMounts;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.VirtualClusters;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.plugins.Image;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.TargetClusterTls;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.targetclustertls.TrustAnchorSecretRef;

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
    void patchSetsAnnotationsAsObjectWhenAnnotationsEmpty() throws Exception {
        Pod pod = podWithAppContainer(Map.of());
        JsonNode patch = createPatchJson(pod);

        assertThat(patchOps(patch, "add", "/metadata/annotations"))
                .as("patch should add the whole annotations object when annotations map is empty")
                .isNotEmpty();
    }

    @Test
    void patchAddsConfigGenerationAnnotation() throws Exception {
        Pod pod = podWithAppContainer(Map.of("existing", "value"));
        JsonNode patch = createPatchJson(pod);

        String escapedKey = PodMutator.escapeJsonPointer(Annotations.CONFIG_GENERATION);
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
        assertThat(secCtx.path("seccompProfile").path("type").asText()).isEqualTo("RuntimeDefault");
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
        bootstrapEnv.setValue("old-kafka:19092");
        app.setEnv(new ArrayList<>(List.of(bootstrapEnv)));

        JsonNode patch = createPatchJson(pod);

        List<JsonNode> replaceOps = patchOps(patch, "replace", "/spec/containers/0/env/0/value");
        assertThat(replaceOps).hasSize(1);
        assertThat(replaceOps.get(0).path("value").asText()).isEqualTo("localhost:" + ProxyConfigGenerator.DEFAULT_BOOTSTRAP_PORT);
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

    // --- Phase 2: Target Cluster TLS ---

    @Test
    void patchAddsTargetClusterTlsVolume() throws Exception {
        Pod pod = podWithAppContainer(null);
        // Give the pod existing volumes so both config and TLS use the append path
        pod.getSpec().setVolumes(new ArrayList<>(List.of(new Volume())));
        KroxyliciousSidecarConfigSpec spec = specWithTargetClusterTls();

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> volumeOps = patchOps(patch, "add", "/spec/volumes/-");
        assertThat(volumeOps).hasSizeGreaterThanOrEqualTo(2);

        boolean hasTlsVolume = volumeOps.stream()
                .anyMatch(op -> PodMutator.TARGET_CLUSTER_TLS_VOLUME_NAME.equals(op.path("value").path("name").asText()));
        assertThat(hasTlsVolume).as("patch should add %s volume", PodMutator.TARGET_CLUSTER_TLS_VOLUME_NAME).isTrue();
    }

    @Test
    void patchAddsTargetClusterTlsVolumeMount() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithTargetClusterTls();

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode mounts = containerOps.get(0).path("value").path("volumeMounts");
        assertThat(mounts).hasSize(2);

        JsonNode tlsMount = mounts.get(1);
        assertThat(tlsMount.path("name").asText()).isEqualTo(PodMutator.TARGET_CLUSTER_TLS_VOLUME_NAME);
        assertThat(tlsMount.path("mountPath").asText()).isEqualTo(PodMutator.TARGET_CLUSTER_TLS_MOUNT_PATH);
        assertThat(tlsMount.path("readOnly").asBoolean()).isTrue();
    }

    @Test
    void noTlsVolumeWhenNotConfigured() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        // Only config volume, not TLS
        boolean hasTlsVolume = false;
        for (JsonNode op : patch) {
            if ("add".equals(op.path("op").asText())) {
                JsonNode value = op.path("value");
                if (value.isObject() && value.path("name").asText().matches(".*tls.*")) {
                    hasTlsVolume = true;
                }
                if (value.isArray()) {
                    for (JsonNode item : value) {
                        if (item.path("name").asText().matches(".*tls.*")) {
                            hasTlsVolume = true;
                        }
                    }
                }
            }
        }
        assertThat(hasTlsVolume).as("no TLS volume expected without target cluster TLS config").isFalse();
    }

    @Test
    void resolveTargetClusterTrustStorePathWithTls() {
        VirtualClusters vc = virtualClusterWithTargetClusterTls();
        String path = PodMutator.resolveTargetClusterTrustStorePath(vc);
        assertThat(path).isEqualTo(PodMutator.TARGET_CLUSTER_TLS_MOUNT_PATH + "/ca.crt");
    }

    @Test
    void resolveTargetClusterTrustStorePathWithoutTls() {
        VirtualClusters vc = defaultVirtualCluster();
        String path = PodMutator.resolveTargetClusterTrustStorePath(vc);
        assertThat(path).isNull();
    }

    // --- Phase 2: Native sidecar ---

    @Test
    void nativeSidecarInjectsAsInitContainer() throws Exception {
        Pod pod = podWithAppContainer(null);
        String patchStr = PodMutator.createPatch(pod, defaultSpec(), PROXY_IMAGE, 0L, true, false);
        JsonNode patch = MAPPER.readTree(patchStr);

        // Should add to initContainers, not containers
        List<JsonNode> initOps = patchOps(patch, "add", "/spec/initContainers");
        assertThat(initOps).isNotEmpty();

        JsonNode container = initOps.get(0).path("value").get(0);
        assertThat(container.path("name").asText()).isEqualTo(InjectionDecision.SIDECAR_CONTAINER_NAME);
        assertThat(container.path("restartPolicy").asText()).isEqualTo("Always");
    }

    @Test
    void regularSidecarDoesNotSetRestartPolicy() throws Exception {
        Pod pod = podWithAppContainer(null);
        String patchStr = PodMutator.createPatch(pod, defaultSpec(), PROXY_IMAGE, 0L, false, false);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        assertThat(containerOps).isNotEmpty();
        assertThat(containerOps.get(0).path("value").has("restartPolicy")).isFalse();
    }

    // --- Phase 4: Plugin volumes ---

    @Test
    void patchAddsOciImageVolumeForPlugin() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithPlugin("my-filter", "reg.io/filter:v1@sha256:abc123");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, true);
        JsonNode patch = MAPPER.readTree(patchStr);

        boolean hasPluginVolume = false;
        for (JsonNode op : patch) {
            if ("add".equals(op.path("op").asText())) {
                JsonNode value = op.path("value");
                if (value.isObject() && "plugin-my-filter".equals(value.path("name").asText())) {
                    assertThat(value.has("image")).isTrue();
                    assertThat(value.path("image").path("reference").asText())
                            .isEqualTo("reg.io/filter:v1@sha256:abc123");
                    hasPluginVolume = true;
                }
            }
        }
        assertThat(hasPluginVolume).as("patch should add OCI image volume for plugin").isTrue();
    }

    @Test
    void patchAddsEmptyDirVolumeForPluginWithoutOciSupport() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithPlugin("my-filter", "reg.io/filter:v1@sha256:abc123");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, false);
        JsonNode patch = MAPPER.readTree(patchStr);

        boolean hasPluginVolume = false;
        for (JsonNode op : patch) {
            if ("add".equals(op.path("op").asText())) {
                JsonNode value = op.path("value");
                if (value.isObject() && "plugin-my-filter".equals(value.path("name").asText())) {
                    assertThat(value.has("emptyDir")).isTrue();
                    assertThat(value.has("image")).isFalse();
                    hasPluginVolume = true;
                }
            }
        }
        assertThat(hasPluginVolume).as("patch should add emptyDir volume for plugin").isTrue();
    }

    @Test
    void patchAddsInitContainerForPluginWithoutOciSupport() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithPlugin("my-filter", "reg.io/filter:v1@sha256:abc123");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, false);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> initOps = patchOps(patch, "add", "/spec/initContainers");
        assertThat(initOps).isNotEmpty();

        JsonNode initContainer = initOps.get(0).path("value").get(0);
        assertThat(initContainer.path("name").asText()).isEqualTo("plugin-my-filter-copy");
        assertThat(initContainer.path("image").asText()).isEqualTo("reg.io/filter:v1@sha256:abc123");
        assertThat(initContainer.path("securityContext").path("allowPrivilegeEscalation").asBoolean()).isFalse();
        assertThat(initContainer.path("securityContext").path("seccompProfile").path("type").asText()).isEqualTo("RuntimeDefault");
    }

    @Test
    void patchDoesNotAddInitContainerForPluginWithOciSupport() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithPlugin("my-filter", "reg.io/filter:v1@sha256:abc123");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, true);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> initOps = patchOps(patch, "add", "/spec/initContainers");
        assertThat(initOps).isEmpty();
    }

    @Test
    void patchAddsPluginVolumeMountWithSubPathForOci() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithPlugin("my-filter", "reg.io/filter:v1@sha256:abc123");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, true);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode mounts = containerOps.get(0).path("value").path("volumeMounts");

        boolean hasPluginMount = false;
        for (JsonNode mount : mounts) {
            if ("plugin-my-filter".equals(mount.path("name").asText())) {
                assertThat(mount.path("mountPath").asText()).isEqualTo("/opt/kroxylicious/classpath-plugins/my-filter");
                assertThat(mount.path("readOnly").asBoolean()).isTrue();
                assertThat(mount.path("subPath").asText()).isEqualTo("plugins");
                hasPluginMount = true;
            }
        }
        assertThat(hasPluginMount).as("sidecar should have plugin volume mount").isTrue();
    }

    @Test
    void patchAddsPluginVolumeMountWithoutSubPathForEmptyDir() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithPlugin("my-filter", "reg.io/filter:v1@sha256:abc123");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, false);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode mounts = containerOps.get(0).path("value").path("volumeMounts");

        boolean hasPluginMount = false;
        for (JsonNode mount : mounts) {
            if ("plugin-my-filter".equals(mount.path("name").asText())) {
                assertThat(mount.path("mountPath").asText()).isEqualTo("/opt/kroxylicious/classpath-plugins/my-filter");
                assertThat(mount.path("readOnly").asBoolean()).isTrue();
                assertThat(mount.has("subPath")).isFalse();
                hasPluginMount = true;
            }
        }
        assertThat(hasPluginMount).as("sidecar should have plugin volume mount").isTrue();
    }

    @Test
    void patchSupportsMultiplePlugins() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = defaultSpec();
        List<Plugins> plugins = new ArrayList<>();
        plugins.add(createPlugin("filter-a", "reg.io/a@sha256:aaa"));
        plugins.add(createPlugin("filter-b", "reg.io/b@sha256:bbb"));
        spec.setPlugins(plugins);

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE, 0L, false, true);
        JsonNode patch = MAPPER.readTree(patchStr);

        int pluginVolumeCount = 0;
        for (JsonNode op : patch) {
            if ("add".equals(op.path("op").asText())) {
                JsonNode value = op.path("value");
                if (value.isObject() && value.path("name").asText().startsWith("plugin-")) {
                    pluginVolumeCount++;
                }
            }
        }
        assertThat(pluginVolumeCount).isEqualTo(2);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode mounts = containerOps.get(0).path("value").path("volumeMounts");
        List<String> mountNames = new ArrayList<>();
        for (JsonNode mount : mounts) {
            mountNames.add(mount.path("name").asText());
        }
        assertThat(mountNames).contains("plugin-filter-a", "plugin-filter-b");
    }

    // --- Phase 5: Secret mounts ---

    @Test
    void patchAddsSecretVolume() throws Exception {
        Pod pod = podWithAppContainer(null);
        pod.getSpec().setVolumes(new ArrayList<>(List.of(new Volume())));
        KroxyliciousSidecarConfigSpec spec = specWithSecretMount("kms", "kms-credentials");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> volumeOps = patchOps(patch, "add", "/spec/volumes/-");
        boolean hasSecretVolume = volumeOps.stream()
                .anyMatch(op -> "secret-kms".equals(op.path("value").path("name").asText())
                        && "kms-credentials".equals(op.path("value").path("secret").path("secretName").asText()));
        assertThat(hasSecretVolume).as("patch should add secret volume").isTrue();
    }

    @Test
    void patchAddsSecretVolumeMount() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = specWithSecretMount("kms", "kms-credentials");

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode mounts = containerOps.get(0).path("value").path("volumeMounts");

        boolean hasSecretMount = false;
        for (JsonNode mount : mounts) {
            if ("secret-kms".equals(mount.path("name").asText())) {
                assertThat(mount.path("mountPath").asText()).isEqualTo("/opt/kroxylicious/secrets/kms");
                assertThat(mount.path("readOnly").asBoolean()).isTrue();
                hasSecretMount = true;
            }
        }
        assertThat(hasSecretMount).as("sidecar should have secret volume mount").isTrue();
    }

    @Test
    void patchSupportsMultipleSecretMounts() throws Exception {
        Pod pod = podWithAppContainer(null);
        KroxyliciousSidecarConfigSpec spec = defaultSpec();
        List<SecretMounts> mounts = new ArrayList<>();
        mounts.add(createSecretMount("kms", "kms-credentials"));
        mounts.add(createSecretMount("auth", "auth-secret"));
        spec.setSecretMounts(mounts);

        String patchStr = PodMutator.createPatch(pod, spec, PROXY_IMAGE);
        JsonNode patch = MAPPER.readTree(patchStr);

        int secretVolumeCount = 0;
        for (JsonNode op : patch) {
            if ("add".equals(op.path("op").asText())) {
                JsonNode value = op.path("value");
                if (value.isObject() && value.path("name").asText().startsWith("secret-")) {
                    secretVolumeCount++;
                }
            }
        }
        assertThat(secretVolumeCount).isEqualTo(2);

        List<JsonNode> containerOps = patchOps(patch, "add", "/spec/containers/-");
        JsonNode volumeMounts = containerOps.get(0).path("value").path("volumeMounts");
        List<String> mountNames = new ArrayList<>();
        for (JsonNode mount : volumeMounts) {
            mountNames.add(mount.path("name").asText());
        }
        assertThat(mountNames).contains("secret-kms", "secret-auth");
    }

    @Test
    void noSecretVolumeWhenNotConfigured() throws Exception {
        Pod pod = podWithAppContainer(null);
        JsonNode patch = createPatchJson(pod);

        boolean hasSecretVolume = false;
        for (JsonNode op : patch) {
            if ("add".equals(op.path("op").asText())) {
                JsonNode value = op.path("value");
                if (value.isObject() && value.path("name").asText().startsWith("secret-")) {
                    hasSecretVolume = true;
                }
                if (value.isArray()) {
                    for (JsonNode item : value) {
                        if (item.path("name").asText().startsWith("secret-")) {
                            hasSecretVolume = true;
                        }
                    }
                }
            }
        }
        assertThat(hasSecretVolume).as("no secret volume expected without secretMounts config").isFalse();
    }

    // --- Skip label patch ---

    @Test
    void skipLabelPatchAddsLabelWhenNoExistingLabels() throws Exception {
        Pod pod = podWithAppContainer(null);
        String patchStr = PodMutator.createSkipLabelPatch(pod, "no-KroxyliciousSidecarConfig");
        JsonNode patch = MAPPER.readTree(patchStr);

        List<JsonNode> ops = patchOps(patch, "add", "/metadata/labels");
        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).path("value").path(Labels.INJECTION_SKIPPED).asText())
                .isEqualTo("no-KroxyliciousSidecarConfig");
    }

    @Test
    void skipLabelPatchAddsLabelWhenExistingLabels() throws Exception {
        Pod pod = podWithAppContainer(null);
        pod.getMetadata().setLabels(new HashMap<>(Map.of("app", "my-app")));
        String patchStr = PodMutator.createSkipLabelPatch(pod, "container-name-conflict");
        JsonNode patch = MAPPER.readTree(patchStr);

        String escapedKey = PodMutator.escapeJsonPointer(Labels.INJECTION_SKIPPED);
        List<JsonNode> ops = patchOps(patch, "add", "/metadata/labels/" + escapedKey);
        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).path("value").asText()).isEqualTo("container-name-conflict");
    }

    // --- helpers ---

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

    private static VirtualClusters defaultVirtualCluster() {
        VirtualClusters vc = new VirtualClusters();
        vc.setName("sidecar");
        vc.setTargetBootstrapServers("kafka.example.com:9092");
        return vc;
    }

    private static KroxyliciousSidecarConfigSpec defaultSpec() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setVirtualClusters(List.of(defaultVirtualCluster()));
        return spec;
    }

    private static VirtualClusters virtualClusterWithTargetClusterTls() {
        VirtualClusters vc = defaultVirtualCluster();
        TargetClusterTls tls = new TargetClusterTls();
        TrustAnchorSecretRef ref = new TrustAnchorSecretRef();
        ref.setName("kafka-ca");
        ref.setKey("ca.crt");
        tls.setTrustAnchorSecretRef(ref);
        vc.setTargetClusterTls(tls);
        return vc;
    }

    private static KroxyliciousSidecarConfigSpec specWithTargetClusterTls() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setVirtualClusters(List.of(virtualClusterWithTargetClusterTls()));
        return spec;
    }

    private JsonNode createPatchJson(Pod pod) throws Exception {
        String patchStr = PodMutator.createPatch(pod, defaultSpec(), PROXY_IMAGE);
        return MAPPER.readTree(patchStr);
    }

    private static KroxyliciousSidecarConfigSpec specWithPlugin(
                                                                String name,
                                                                String reference) {
        KroxyliciousSidecarConfigSpec spec = defaultSpec();
        spec.setPlugins(new ArrayList<>(List.of(createPlugin(name, reference))));
        return spec;
    }

    private static Plugins createPlugin(
                                        String name,
                                        String reference) {
        Plugins plugin = new Plugins();
        plugin.setName(name);
        Image image = new Image();
        image.setReference(reference);
        plugin.setImage(image);
        return plugin;
    }

    private static KroxyliciousSidecarConfigSpec specWithSecretMount(
                                                                     String name,
                                                                     String secretName) {
        KroxyliciousSidecarConfigSpec spec = defaultSpec();
        spec.setSecretMounts(new ArrayList<>(List.of(createSecretMount(name, secretName))));
        return spec;
    }

    private static SecretMounts createSecretMount(
                                                  String name,
                                                  String secretName) {
        SecretMounts sm = new SecretMounts();
        sm.setName(name);
        sm.setSecretName(secretName);
        return sm;
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
