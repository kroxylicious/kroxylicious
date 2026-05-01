/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.Plugins;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.UpstreamTls;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.upstreamtls.TrustAnchorSecretRef;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Generates a JSON Patch (RFC 6902) to inject a Kroxylicious sidecar into a pod.
 */
class PodMutator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SIDECAR_VOLUME_NAME = "kroxylicious-config";
    private static final String UPSTREAM_TLS_VOLUME_NAME = "upstream-tls";
    @SuppressWarnings("java:S1075") // there's nothing wrong with hard coding this path.
    private static final String UPSTREAM_TLS_MOUNT_PATH = "/opt/kroxylicious/tls/upstream";
    @SuppressWarnings("java:S1075") // there's nothing wrong with hard coding this path.
    private static final String PLUGINS_BASE_PATH = "/opt/kroxylicious/classpath-plugins";
    private static final String PLUGIN_VOLUME_PREFIX = "plugin-";
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
                              boolean useNativeSidecar,
                              boolean useOciImageVolumes) {
        try {
            ArrayNode patch = MAPPER.createArrayNode();

            String upstreamTrustStorePath = resolveUpstreamTrustStorePath(spec);
            String proxyConfig = ProxyConfigGenerator.generateConfig(spec, upstreamTrustStorePath);
            int bootstrapPort = ProxyConfigGenerator.resolveBootstrapPort(spec);
            int managementPort = ProxyConfigGenerator.resolveManagementPort(spec);

            addAnnotationOps(patch, pod, proxyConfig);
            addVolumeOps(patch, pod, spec, useOciImageVolumes);
            addPluginCopyInitContainers(patch, pod, spec, useOciImageVolumes);
            addSidecarContainerOp(patch, pod, proxyImage, managementPort, spec,
                    useNativeSidecar, useOciImageVolumes);
            addBootstrapEnvVarOps(patch, pod, spec, bootstrapPort);
            addSecurityContextOps(patch, pod);

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
        return createPatch(pod, spec, proxyImage, false, false);
    }

    /**
     * Computes the path where the upstream CA cert will be mounted inside the sidecar,
     * or null if upstream TLS is not configured.
     */
    @Nullable
    static String resolveUpstreamTrustStorePath(KroxyliciousSidecarConfigSpec spec) {
        UpstreamTls tls = spec.getUpstreamTls();
        if (tls == null || tls.getTrustAnchorSecretRef() == null) {
            return null;
        }
        return UPSTREAM_TLS_MOUNT_PATH + "/" + tls.getTrustAnchorSecretRef().getKey();
    }

    private static void addAnnotationOps(
                                         ArrayNode patch,
                                         Pod pod,
                                         String proxyConfig) {

        Map<String, String> existing = pod.getMetadata() != null ? pod.getMetadata().getAnnotations() : null;
        if (existing == null || existing.isEmpty()) {
            ObjectNode annotations = MAPPER.createObjectNode();
            annotations.put(Annotations.PROXY_CONFIG, proxyConfig);
            annotations.put(Annotations.SIDECAR_STATUS, "injected");
            addOp(patch, OP_ADD, "/metadata/annotations", annotations);
        }
        else {
            addOp(patch, OP_ADD, "/metadata/annotations/" + escapeJsonPointer(Annotations.PROXY_CONFIG), proxyConfig);
            addOp(patch, OP_ADD, "/metadata/annotations/" + escapeJsonPointer(Annotations.SIDECAR_STATUS), "injected");
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

        ObjectNode configVolume = buildProxyConfigVolume();
        if (hasVolumes) {
            addOp(patch, OP_ADD, "/spec/volumes/-", configVolume);
        }
        else {
            ArrayNode volumes = MAPPER.createArrayNode();
            volumes.add(configVolume);
            addOp(patch, OP_ADD, "/spec/volumes", volumes);
        }

        // Upstream TLS volume (Secret)
        UpstreamTls tls = spec.getUpstreamTls();
        if (tls != null && tls.getTrustAnchorSecretRef() != null) {
            TrustAnchorSecretRef secretRef = tls.getTrustAnchorSecretRef();
            ObjectNode tlsVolume = MAPPER.createObjectNode();
            tlsVolume.put("name", UPSTREAM_TLS_VOLUME_NAME);
            ObjectNode secret = tlsVolume.putObject("secret");
            secret.put("secretName", secretRef.getName());
            addOp(patch, OP_ADD, "/spec/volumes/-", tlsVolume);
        }

        // Plugin volumes
        List<Plugins> plugins = spec.getPlugins();
        if (plugins != null) {
            for (Plugins plugin : plugins) {
                addOp(patch, OP_ADD, "/spec/volumes/-", buildPluginVolume(useOciImageVolumes, plugin));
            }
        }
    }

    @NonNull
    private static ObjectNode buildPluginVolume(boolean useOciImageVolumes, Plugins plugin) {
        ObjectNode pluginVolume = MAPPER.createObjectNode();
        pluginVolume.put("name", PLUGIN_VOLUME_PREFIX + plugin.getName());

        if (useOciImageVolumes) {
            ObjectNode image = pluginVolume.putObject("image");
            image.put("reference", plugin.getImage().getReference());
            if (plugin.getImage().getPullPolicy() != null) {
                image.put("pullPolicy", plugin.getImage().getPullPolicy().getValue());
            }
        }
        else {
            // Fallback: emptyDir volume, populated by an init container
            pluginVolume.putObject("emptyDir");
        }
        return pluginVolume;
    }

    @NonNull
    private static ObjectNode buildProxyConfigVolume() {
        // Use downwardAPI to mount a volume such that the container's proxy-config.yaml has the content of the
        // kroxylicious.io/proxy-config annotation's value.
        ObjectNode configVolume = MAPPER.createObjectNode();
        configVolume.put("name", SIDECAR_VOLUME_NAME);
        ObjectNode downwardAPI = configVolume.putObject("downwardAPI");
        ArrayNode items = downwardAPI.putArray("items");
        ObjectNode item = items.addObject();
        item.put("path", CONFIG_FILE_NAME);
        ObjectNode fieldRef = item.putObject("fieldRef");
        fieldRef.put("fieldPath", "metadata.annotations['" + Annotations.PROXY_CONFIG + "']");
        return configVolume;
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
            ObjectNode initContainer = MAPPER.createObjectNode();
            initContainer.put("name", PLUGIN_VOLUME_PREFIX + plugin.getName() + "-copy");
            initContainer.put("image", plugin.getImage().getReference());

            ArrayNode command = initContainer.putArray("command");
            command.add("sh");
            command.add("-c");
            command.add("cp -r /. /plugins/");

            ArrayNode mounts = initContainer.putArray("volumeMounts");
            ObjectNode mount = mounts.addObject();
            mount.put("name", PLUGIN_VOLUME_PREFIX + plugin.getName());
            mount.put("mountPath", "/plugins");

            // Security context for init container
            ObjectNode secCtx = initContainer.putObject("securityContext");
            secCtx.put("allowPrivilegeEscalation", false);
            secCtx.put("readOnlyRootFilesystem", true);
            ObjectNode capabilities = secCtx.putObject("capabilities");
            capabilities.putArray("drop").add("ALL");

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

        ObjectNode container = MAPPER.createObjectNode();
        container.put("name", InjectionDecision.SIDECAR_CONTAINER_NAME);
        container.put("image", proxyImage);

        if (useNativeSidecar) {
            container.put("restartPolicy", "Always");
        }

        ArrayNode args = container.putArray("args");
        args.add("--config");
        args.add(CONFIG_MOUNT_PATH);

        // Security context
        ObjectNode secCtx = container.putObject("securityContext");
        secCtx.put("allowPrivilegeEscalation", false);
        secCtx.put("readOnlyRootFilesystem", true);
        ObjectNode capabilities = secCtx.putObject("capabilities");
        ArrayNode drop = capabilities.putArray("drop");
        drop.add("ALL");

        // Ports
        ArrayNode ports = container.putArray("ports");
        ObjectNode mgmtPort = ports.addObject();
        mgmtPort.put("containerPort", managementPort);
        mgmtPort.put("name", MANAGEMENT_PORT_NAME);
        mgmtPort.put("protocol", "TCP");

        // Probes
        addProbe(container, "startupProbe", 5, 2, 30);
        addProbe(container, "livenessProbe", 30, 10, 3);
        addProbe(container, "readinessProbe", 5, 2, 5);

        // Volume mounts
        ArrayNode volumeMounts = container.putArray("volumeMounts");
        ObjectNode configMount = volumeMounts.addObject();
        configMount.put("name", SIDECAR_VOLUME_NAME);
        configMount.put("mountPath", CONFIG_MOUNT_PATH);
        configMount.put("subPath", CONFIG_FILE_NAME);
        configMount.put("readOnly", true);

        // Upstream TLS volume mount
        if (spec.getUpstreamTls() != null && spec.getUpstreamTls().getTrustAnchorSecretRef() != null) {
            ObjectNode tlsMount = volumeMounts.addObject();
            tlsMount.put("name", UPSTREAM_TLS_VOLUME_NAME);
            tlsMount.put("mountPath", UPSTREAM_TLS_MOUNT_PATH);
            tlsMount.put("readOnly", true);
        }

        // Plugin volume mounts
        List<Plugins> plugins = spec.getPlugins();
        if (plugins != null) {
            for (Plugins plugin : plugins) {
                ObjectNode pluginMount = volumeMounts.addObject();
                pluginMount.put("name", PLUGIN_VOLUME_PREFIX + plugin.getName());
                pluginMount.put("mountPath", PLUGINS_BASE_PATH + "/" + plugin.getName());
                pluginMount.put("readOnly", true);
            }
        }

        // Resources
        if (spec.getResources() != null) {
            try {
                String resourcesJson = MAPPER.writeValueAsString(spec.getResources());
                container.set("resources", MAPPER.readTree(resourcesJson));
            }
            catch (JsonProcessingException e) {
                // Ignore resource serialization failures — sidecar will run without limits
            }
        }

        container.put("terminationMessagePolicy", "FallbackToLogsOnError");

        if (useNativeSidecar) {
            addInitContainer(patch, pod, container);
        }
        else {
            addRegularContainer(patch, pod, container);
        }
    }

    private static void addRegularContainer(ArrayNode patch, Pod pod, ObjectNode container) {
        boolean hasContainers = pod.getSpec() != null
                && pod.getSpec().getContainers() != null
                && !pod.getSpec().getContainers().isEmpty();

        if (hasContainers) {
            addOp(patch, OP_ADD, "/spec/containers/-", container);
        }
        else {
            ArrayNode containers = MAPPER.createArrayNode();
            containers.add(container);
            addOp(patch, OP_ADD, "/spec/containers", containers);
        }
    }

    private static void addInitContainer(ArrayNode patch, Pod pod, ObjectNode container) {
        boolean hasInitContainers = pod.getSpec() != null
                && pod.getSpec().getInitContainers() != null
                && !pod.getSpec().getInitContainers().isEmpty();

        if (hasInitContainers) {
            addOp(patch, OP_ADD, "/spec/initContainers/-", container);
        }
        else {
            ArrayNode initContainers = MAPPER.createArrayNode();
            initContainers.add(container);
            addOp(patch, OP_ADD, "/spec/initContainers", initContainers);
        }
    }

    private static void addProbe(
                                 ObjectNode container,
                                 String probeName,
                                 int initialDelay,
                                 int period,
                                 int failureThreshold) {

        ObjectNode probe = container.putObject(probeName);
        ObjectNode httpGet = probe.putObject("httpGet");
        httpGet.put("path", "/livez");
        httpGet.put("port", MANAGEMENT_PORT_NAME);
        probe.put("initialDelaySeconds", initialDelay);
        probe.put("periodSeconds", period);
        probe.put("failureThreshold", failureThreshold);
        probe.put("timeoutSeconds", 1);
    }

    @SuppressWarnings("java:S1192") // dupe strings:
    // "value": env var "value" here is not the same as json path op "value" later on
    // "/spec/containers/": could use a constant, but it would make the code harder to understand
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
        List<io.fabric8.kubernetes.api.model.Container> containers = pod.getSpec().getContainers();

        for (int i = 0; i < containers.size(); i++) {
            io.fabric8.kubernetes.api.model.Container c = containers.get(i);
            if (InjectionDecision.SIDECAR_CONTAINER_NAME.equals(c.getName())) {
                continue;
            }

            boolean hasEnv = c.getEnv() != null && !c.getEnv().isEmpty();
            ObjectNode envVar = MAPPER.createObjectNode();
            envVar.put("name", KAFKA_BOOTSTRAP_SERVERS_ENV);
            envVar.put("value", bootstrapValue);

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
                            envVar);
                }
            }
            else {
                ArrayNode envArray = MAPPER.createArrayNode();
                envArray.add(envVar);
                addOp(patch, OP_ADD,
                        "/spec/containers/" + i + "/env",
                        envArray);
            }
        }
    }

    // TODO add an import and avoid the fq name below
    private static int findEnvVarIndex(
                                       List<io.fabric8.kubernetes.api.model.EnvVar> env,
                                       String name) {
        for (int i = 0; i < env.size(); i++) {
            if (name.equals(env.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    private static void addSecurityContextOps(ArrayNode patch, Pod pod) {
        if (pod.getSpec() == null) {
            return;
        }

        if (pod.getSpec().getSecurityContext() == null) {
            ObjectNode secCtx = MAPPER.createObjectNode();
            secCtx.put("runAsNonRoot", true);
            ObjectNode seccompProfile = secCtx.putObject("seccompProfile");
            seccompProfile.put("type", "RuntimeDefault");
            addOp(patch, OP_ADD, "/spec/securityContext", secCtx);
        }
    }

    @SuppressWarnings("java:S1192") // dupe "value" strings, but env var "value" not the same as json path op "value"
    private static void addOp(ArrayNode patch, String op, String path, Object value) {
        ObjectNode opNode = MAPPER.createObjectNode();
        opNode.put("op", op);
        opNode.put("path", path);
        if (value instanceof String s) {
            opNode.put("value", s);
        }
        else if (value instanceof com.fasterxml.jackson.databind.JsonNode jsonNode) {
            opNode.set("value", jsonNode);
        }
        patch.add(opNode);
    }

    /**
     * Escapes a string for use in a JSON Pointer (RFC 6901).
     * {@code ~} is escaped as {@code ~0}, {@code /} is escaped as {@code ~1}.
     */
    static String escapeJsonPointer(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }
}
