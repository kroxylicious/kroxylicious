/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ImageVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.Plugins;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.SecretMounts;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.VirtualClusters;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.TargetClusterTls;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.targetclustertls.TrustAnchorSecretRef;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Generates a JSON Patch (RFC 6902) to inject a Kroxylicious sidecar into a pod.
 */
class PodMutator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SIDECAR_VOLUME_NAME = "kroxylicious-config";
    static final String TARGET_CLUSTER_TLS_VOLUME_NAME = "target-cluster-tls";
    @SuppressWarnings("java:S1075") // there's nothing wrong with hard coding this path.
    static final String TARGET_CLUSTER_TLS_MOUNT_PATH = "/opt/kroxylicious/tls/target";
    @SuppressWarnings("java:S1075") // there's nothing wrong with hard coding this path.
    private static final String PLUGINS_BASE_PATH = "/opt/kroxylicious/classpath-plugins";
    private static final String PLUGIN_VOLUME_PREFIX = "plugin-";
    /** Path within plugin OCI images where JARs must be placed. */
    private static final String PLUGIN_IMAGE_JAR_PATH = "/plugins";
    /** Mount path for the emptyDir volume inside plugin-copy init containers. */
    private static final String PLUGIN_COPY_DEST_PATH = "/dest";
    private static final String SECRET_VOLUME_PREFIX = "secret-";
    @SuppressWarnings("java:S1075")
    private static final String SECRETS_BASE_PATH = "/opt/kroxylicious/secrets";
    @SuppressWarnings("java:S1075") // there's nothing wrong with hard coding this path.
    private static final String CONFIG_MOUNT_PATH = "/opt/kroxylicious/config/proxy-config.yaml";
    private static final String CONFIG_FILE_NAME = "proxy-config.yaml";
    private static final String MANAGEMENT_PORT_NAME = "management";
    private static final String KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String OP_ADD = "add";
    public static final String OP_REPLACE = "replace";

    private PodMutator() {
    }

    /**
     * Generates a JSON Patch to inject the sidecar into the pod.
     *
     * @param pod the original pod
     * @param spec the sidecar configuration
     * @param proxyImage the container image to use for the proxy
     * @param configGeneration the {@code metadata.generation} of the {@code KroxyliciousSidecarConfig}
     * @param useNativeSidecar if true, inject as an init container with restartPolicy: Always (K8s 1.28+)
     * @param useOciImageVolumes if true, mount plugin images as OCI image volumes (K8s 1.31+);
     *                          otherwise use init-container + emptyDir fallback
     * @return JSON Patch string (RFC 6902)
     */
    @NonNull
    static String createPatch(
                              @NonNull Pod pod,
                              @NonNull KroxyliciousSidecarConfigSpec spec,
                              @NonNull String proxyImage,
                              long configGeneration,
                              boolean useNativeSidecar,
                              boolean useOciImageVolumes) {
        try {
            ArrayNode patch = MAPPER.createArrayNode();

            VirtualClusters vc = spec.getVirtualClusters().get(0);
            String targetClusterTrustStorePath = resolveTargetClusterTrustStorePath(vc);
            String proxyConfig = ProxyConfigGenerator.generateConfig(spec, targetClusterTrustStorePath);
            int bootstrapPort = ProxyConfigGenerator.resolveBootstrapPort(vc);
            int managementPort = ProxyConfigGenerator.resolveManagementPort(spec);

            addAnnotationOps(patch, pod, proxyConfig, configGeneration);
            addVolumeOps(patch, pod, spec, useOciImageVolumes);
            addPluginCopyInitContainers(patch, pod, spec, useOciImageVolumes);
            addSidecarContainerOp(patch, pod, proxyImage, managementPort, spec,
                    useNativeSidecar, useOciImageVolumes);
            addBootstrapEnvVarOps(patch, pod, spec, bootstrapPort);

            return MAPPER.writeValueAsString(patch);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise JSON patch", e);
        }
    }

    /**
     * Overload for backwards compatibility (no plugins).
     */
    @NonNull
    static String createPatch(
                              @NonNull Pod pod,
                              @NonNull KroxyliciousSidecarConfigSpec spec,
                              @NonNull String proxyImage) {
        return createPatch(pod, spec, proxyImage, 0L, false, false);
    }

    /**
     * Computes the path where the target cluster CA cert will be mounted inside the sidecar,
     * or null if target cluster TLS is not configured.
     */
    @Nullable
    static String resolveTargetClusterTrustStorePath(VirtualClusters vc) {
        TargetClusterTls tls = vc.getTargetClusterTls();
        if (tls == null || tls.getTrustAnchorSecretRef() == null) {
            return null;
        }
        return TARGET_CLUSTER_TLS_MOUNT_PATH + "/" + tls.getTrustAnchorSecretRef().getKey();
    }

    private static void addAnnotationOps(
                                         ArrayNode patch,
                                         Pod pod,
                                         String proxyConfig,
                                         long configGeneration) {

        String generationStr = Long.toString(configGeneration);
        Map<String, String> existing = pod.getMetadata() != null ? pod.getMetadata().getAnnotations() : null;
        if (existing == null || existing.isEmpty()) {
            addOp(patch, OP_ADD, "/metadata/annotations",
                    toJson(Map.of(
                            Annotations.PROXY_CONFIG, proxyConfig,
                            Annotations.CONFIG_GENERATION, generationStr)));
        }
        else {
            addOp(patch, OP_ADD, "/metadata/annotations/" + escapeJsonPointer(Annotations.PROXY_CONFIG), proxyConfig);
            addOp(patch, OP_ADD, "/metadata/annotations/" + escapeJsonPointer(Annotations.CONFIG_GENERATION), generationStr);
        }
    }

    private static void addVolumeOps(
                                     ArrayNode patch,
                                     Pod pod,
                                     KroxyliciousSidecarConfigSpec spec,
                                     boolean useOciImageVolumes) {
        boolean hasVolumes = pod.getSpec() != null
                && pod.getSpec().getVolumes() != null
                && !pod.getSpec().getVolumes().isEmpty();

        Volume configVolume = buildProxyConfigVolume();
        if (hasVolumes) {
            addOp(patch, OP_ADD, "/spec/volumes/-", toJson(configVolume));
        }
        else {
            addOp(patch, OP_ADD, "/spec/volumes", toJson(List.of(configVolume)));
        }

        VirtualClusters vc = spec.getVirtualClusters().get(0);
        TargetClusterTls tls = vc.getTargetClusterTls();
        if (tls != null && tls.getTrustAnchorSecretRef() != null) {
            addOp(patch, OP_ADD, "/spec/volumes/-", toJson(buildTlsSecretVolume(tls)));
        }

        List<SecretMounts> secretMounts = spec.getSecretMounts();
        if (secretMounts != null) {
            for (SecretMounts sm : secretMounts) {
                addOp(patch, OP_ADD, "/spec/volumes/-", toJson(buildSecretVolume(sm)));
            }
        }

        List<Plugins> plugins = spec.getPlugins();
        if (plugins != null) {
            for (Plugins plugin : plugins) {
                addOp(patch, OP_ADD, "/spec/volumes/-", toJson(buildPluginVolume(useOciImageVolumes, plugin)));
            }
        }
    }

    @NonNull
    private static Volume buildTlsSecretVolume(TargetClusterTls tls) {
        TrustAnchorSecretRef secretRef = tls.getTrustAnchorSecretRef();
        return new VolumeBuilder()
                .withName(TARGET_CLUSTER_TLS_VOLUME_NAME)
                .withNewSecret()
                .withSecretName(secretRef.getName())
                .endSecret()
                .build();
    }

    @NonNull
    private static Volume buildSecretVolume(SecretMounts sm) {
        return new VolumeBuilder()
                .withName(SECRET_VOLUME_PREFIX + sm.getName())
                .withNewSecret()
                .withSecretName(sm.getSecretName())
                .endSecret()
                .build();
    }

    @NonNull
    private static Volume buildPluginVolume(boolean useOciImageVolumes, Plugins plugin) {
        VolumeBuilder builder = new VolumeBuilder()
                .withName(PLUGIN_VOLUME_PREFIX + plugin.getName());

        if (useOciImageVolumes) {
            ImageVolumeSourceBuilder imageBuilder = new ImageVolumeSourceBuilder()
                    .withReference(plugin.getImage().getReference());
            if (plugin.getImage().getPullPolicy() != null) {
                imageBuilder.withPullPolicy(plugin.getImage().getPullPolicy().getValue());
            }
            builder.withImage(imageBuilder.build());
        }
        else {
            builder.withEmptyDir(new EmptyDirVolumeSource());
        }
        return builder.build();
    }

    @NonNull
    private static Volume buildProxyConfigVolume() {
        return new VolumeBuilder()
                .withName(SIDECAR_VOLUME_NAME)
                .withNewDownwardAPI()
                .addNewItem()
                .withPath(CONFIG_FILE_NAME)
                .withNewFieldRef()
                .withFieldPath("metadata.annotations['" + Annotations.PROXY_CONFIG + "']")
                .endFieldRef()
                .endItem()
                .endDownwardAPI()
                .build();
    }

    /**
     * Adds init containers that copy plugin JARs from OCI images to emptyDir volumes.
     * Only used when OCI image volumes are not available.
     */
    private static void addPluginCopyInitContainers(
                                                    ArrayNode patch,
                                                    Pod pod,
                                                    KroxyliciousSidecarConfigSpec spec,
                                                    boolean useOciImageVolumes) {

        List<Plugins> plugins = spec.getPlugins();
        if (plugins == null || plugins.isEmpty() || useOciImageVolumes) {
            return;
        }

        for (Plugins plugin : plugins) {
            ContainerBuilder cb = new ContainerBuilder()
                    .withName(PLUGIN_VOLUME_PREFIX + plugin.getName() + "-copy")
                    .withImage(plugin.getImage().getReference())
                    .withCommand("sh", "-c",
                            "cp " + PLUGIN_IMAGE_JAR_PATH + "/*.jar "
                                    + PLUGIN_COPY_DEST_PATH + "/");
            if (plugin.getImage().getPullPolicy() != null) {
                cb.withImagePullPolicy(plugin.getImage().getPullPolicy().getValue());
            }
            Container initContainer = cb
                    .addNewVolumeMount()
                    .withName(PLUGIN_VOLUME_PREFIX + plugin.getName())
                    .withMountPath(PLUGIN_COPY_DEST_PATH)
                    .endVolumeMount()
                    .withNewSecurityContext()
                    .withAllowPrivilegeEscalation(false)
                    .withReadOnlyRootFilesystem(true)
                    .withNewCapabilities()
                    .addToDrop("ALL")
                    .endCapabilities()
                    .withNewSeccompProfile()
                    .withType("RuntimeDefault")
                    .endSeccompProfile()
                    .endSecurityContext()
                    .build();

            addInitContainer(patch, pod, initContainer);
        }
    }

    private static void addSidecarContainerOp(
                                              ArrayNode patch,
                                              Pod pod,
                                              String proxyImage,
                                              int managementPort,
                                              KroxyliciousSidecarConfigSpec spec,
                                              boolean useNativeSidecar,
                                              boolean useOciImageVolumes) {

        ContainerBuilder builder = new ContainerBuilder()
                .withName(InjectionDecision.SIDECAR_CONTAINER_NAME)
                .withImage(proxyImage);

        if (useNativeSidecar) {
            builder.withRestartPolicy("Always");
        }

        builder.withArgs("--config", CONFIG_MOUNT_PATH)
                .withNewSecurityContext()
                .withAllowPrivilegeEscalation(false)
                .withReadOnlyRootFilesystem(true)
                .withNewCapabilities()
                .addToDrop("ALL")
                .endCapabilities()
                .withNewSeccompProfile()
                .withType("RuntimeDefault")
                .endSeccompProfile()
                .endSecurityContext()
                .addNewPort()
                .withContainerPort(managementPort)
                .withName(MANAGEMENT_PORT_NAME)
                .withProtocol("TCP")
                .endPort()
                .withStartupProbe(buildProbe(5, 2, 30))
                .withLivenessProbe(buildProbe(30, 10, 3))
                .withReadinessProbe(buildProbe(5, 2, 5));

        builder.addNewVolumeMount()
                .withName(SIDECAR_VOLUME_NAME)
                .withMountPath(CONFIG_MOUNT_PATH)
                .withSubPath(CONFIG_FILE_NAME)
                .withReadOnly(true)
                .endVolumeMount();

        VirtualClusters vcForTls = spec.getVirtualClusters().get(0);
        if (vcForTls.getTargetClusterTls() != null && vcForTls.getTargetClusterTls().getTrustAnchorSecretRef() != null) {
            builder.addNewVolumeMount()
                    .withName(TARGET_CLUSTER_TLS_VOLUME_NAME)
                    .withMountPath(TARGET_CLUSTER_TLS_MOUNT_PATH)
                    .withReadOnly(true)
                    .endVolumeMount();
        }

        List<SecretMounts> secretMounts = spec.getSecretMounts();
        if (secretMounts != null) {
            for (SecretMounts sm : secretMounts) {
                builder.addNewVolumeMount()
                        .withName(SECRET_VOLUME_PREFIX + sm.getName())
                        .withMountPath(SECRETS_BASE_PATH + "/" + sm.getName())
                        .withReadOnly(true)
                        .endVolumeMount();
            }
        }

        List<Plugins> plugins = spec.getPlugins();
        if (plugins != null) {
            for (Plugins plugin : plugins) {
                var mount = builder.addNewVolumeMount()
                        .withName(PLUGIN_VOLUME_PREFIX + plugin.getName())
                        .withMountPath(PLUGINS_BASE_PATH + "/" + plugin.getName())
                        .withReadOnly(true);
                if (useOciImageVolumes) {
                    mount.withSubPath(PLUGIN_IMAGE_JAR_PATH.substring(1));
                }
                mount.endVolumeMount();
            }
        }

        if (spec.getResources() != null) {
            builder.withResources(spec.getResources());
        }

        builder.withTerminationMessagePolicy("FallbackToLogsOnError");

        Container container = builder.build();
        if (useNativeSidecar) {
            addInitContainer(patch, pod, container);
        }
        else {
            addRegularContainer(patch, pod, container);
        }
    }

    private static void addRegularContainer(ArrayNode patch, Pod pod, Container container) {
        boolean hasContainers = pod.getSpec() != null
                && pod.getSpec().getContainers() != null
                && !pod.getSpec().getContainers().isEmpty();

        if (hasContainers) {
            addOp(patch, OP_ADD, "/spec/containers/-", toJson(container));
        }
        else {
            addOp(patch, OP_ADD, "/spec/containers", toJson(List.of(container)));
        }
    }

    private static void addInitContainer(ArrayNode patch, Pod pod, Container container) {
        boolean hasInitContainers = pod.getSpec() != null
                && pod.getSpec().getInitContainers() != null
                && !pod.getSpec().getInitContainers().isEmpty();

        if (hasInitContainers) {
            addOp(patch, OP_ADD, "/spec/initContainers/-", toJson(container));
        }
        else {
            addOp(patch, OP_ADD, "/spec/initContainers", toJson(List.of(container)));
        }
    }

    private static Probe buildProbe(
                                    int initialDelay,
                                    int period,
                                    int failureThreshold) {
        return new ProbeBuilder()
                .withNewHttpGet()
                .withPath("/livez")
                .withPort(new IntOrString(MANAGEMENT_PORT_NAME))
                .endHttpGet()
                .withInitialDelaySeconds(initialDelay)
                .withPeriodSeconds(period)
                .withFailureThreshold(failureThreshold)
                .withTimeoutSeconds(1)
                .build();
    }

    private static void addBootstrapEnvVarOps(
                                              ArrayNode patch,
                                              Pod pod,
                                              KroxyliciousSidecarConfigSpec spec,
                                              int bootstrapPort) {

        Boolean setEnvVar = spec.getSetBootstrapEnvVar();
        if (setEnvVar != null && !setEnvVar) {
            return;
        }

        if (pod.getSpec() == null || pod.getSpec().getContainers() == null) {
            return;
        }

        String bootstrapValue = "localhost:" + bootstrapPort;
        List<Container> containers = pod.getSpec().getContainers();

        for (int i = 0; i < containers.size(); i++) {
            Container c = containers.get(i);
            if (InjectionDecision.SIDECAR_CONTAINER_NAME.equals(c.getName())) {
                continue;
            }

            boolean hasEnv = c.getEnv() != null && !c.getEnv().isEmpty();
            EnvVar envVar = new EnvVarBuilder()
                    .withName(KAFKA_BOOTSTRAP_SERVERS_ENV)
                    .withValue(bootstrapValue)
                    .build();

            if (hasEnv) {
                int existingIdx = findEnvVarIndex(c.getEnv(), KAFKA_BOOTSTRAP_SERVERS_ENV);
                if (existingIdx >= 0) {
                    addOp(patch, OP_REPLACE,
                            "/spec/containers/" + i + "/env/" + existingIdx + "/value",
                            bootstrapValue);
                }
                else {
                    addOp(patch, OP_ADD,
                            "/spec/containers/" + i + "/env/-",
                            toJson(envVar));
                }
            }
            else {
                addOp(patch, OP_ADD,
                        "/spec/containers/" + i + "/env",
                        toJson(List.of(envVar)));
            }
        }
    }

    private static int findEnvVarIndex(
                                       List<EnvVar> env,
                                       String name) {
        for (int i = 0; i < env.size(); i++) {
            if (name.equals(env.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    private static JsonNode toJson(Object value) {
        return MAPPER.valueToTree(value);
    }

    private static void addOp(ArrayNode patch, String op, String path, String value) {
        ObjectNode opNode = MAPPER.createObjectNode();
        opNode.put("op", op);
        opNode.put("path", path);
        opNode.put("value", value);
        patch.add(opNode);
    }

    private static void addOp(ArrayNode patch, String op, String path, JsonNode value) {
        ObjectNode opNode = MAPPER.createObjectNode();
        opNode.put("op", op);
        opNode.put("path", path);
        opNode.set("value", value);
        patch.add(opNode);
    }

    /**
     * Generates a JSON Patch that adds the {@code injection-skipped} label to a pod.
     */
    @NonNull
    static String createSkipLabelPatch(
                                       @NonNull Pod pod,
                                       @NonNull String reason) {
        try {
            ArrayNode patch = MAPPER.createArrayNode();
            Map<String, String> existing = pod.getMetadata() != null ? pod.getMetadata().getLabels() : null;
            if (existing == null || existing.isEmpty()) {
                addOp(patch, OP_ADD, "/metadata/labels",
                        toJson(Map.of(Labels.INJECTION_SKIPPED, reason)));
            }
            else {
                addOp(patch, OP_ADD,
                        "/metadata/labels/" + escapeJsonPointer(Labels.INJECTION_SKIPPED),
                        reason);
            }
            return MAPPER.writeValueAsString(patch);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise JSON patch", e);
        }
    }

    /**
     * Escapes a string for use in a JSON Pointer (RFC 6901).
     * {@code ~} is escaped as {@code ~0}, {@code /} is escaped as {@code ~1}.
     */
    static String escapeJsonPointer(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }
}
