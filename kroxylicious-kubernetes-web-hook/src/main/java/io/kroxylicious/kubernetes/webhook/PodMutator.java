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

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Generates a JSON Patch (RFC 6902) to inject a Kroxylicious sidecar into a pod.
 */
class PodMutator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SIDECAR_VOLUME_NAME = "kroxylicious-config";
    private static final String CONFIG_MOUNT_PATH = "/opt/kroxylicious/config/proxy-config.yaml";
    private static final String CONFIG_FILE_NAME = "proxy-config.yaml";
    private static final String MANAGEMENT_PORT_NAME = "management";
    private static final String KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";

    private PodMutator() {
    }

    /**
     * Generates a JSON Patch to inject the sidecar into the pod.
     *
     * @param pod the original pod
     * @param spec the sidecar configuration
     * @param proxyImage the container image to use for the proxy
     * @return JSON Patch string (RFC 6902)
     */
    @NonNull
    static String createPatch(
                              @NonNull Pod pod,
                              @NonNull KroxyliciousSidecarConfigSpec spec,
                              @NonNull String proxyImage) {
        try {
            ArrayNode patch = MAPPER.createArrayNode();

            String proxyConfig = ProxyConfigGenerator.generateConfig(spec);
            int bootstrapPort = ProxyConfigGenerator.resolveBootstrapPort(spec);
            int managementPort = ProxyConfigGenerator.resolveManagementPort(spec);

            addAnnotationOps(patch, pod, proxyConfig);
            addVolumeOps(patch, pod);
            addSidecarContainerOp(patch, pod, proxyImage, managementPort, spec);
            addBootstrapEnvVarOps(patch, pod, spec, bootstrapPort);
            addSecurityContextOps(patch, pod);

            return MAPPER.writeValueAsString(patch);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise JSON patch", e);
        }
    }

    private static void addAnnotationOps(
                                         ArrayNode patch,
                                         Pod pod,
                                         String proxyConfig) {

        Map<String, String> existing = pod.getMetadata() != null ? pod.getMetadata().getAnnotations() : null;
        if (existing == null) {
            // annotations map doesn't exist yet — add it
            ObjectNode annotations = MAPPER.createObjectNode();
            annotations.put(Annotations.PROXY_CONFIG, proxyConfig);
            annotations.put(Annotations.SIDECAR_STATUS, "injected");
            addOp(patch, "add", "/metadata/annotations", annotations);
        }
        else {
            addOp(patch, "add", "/metadata/annotations/" + escapeJsonPointer(Annotations.PROXY_CONFIG), proxyConfig);
            addOp(patch, "add", "/metadata/annotations/" + escapeJsonPointer(Annotations.SIDECAR_STATUS), "injected");
        }
    }

    private static void addVolumeOps(ArrayNode patch, Pod pod) {
        boolean hasVolumes = pod.getSpec() != null
                && pod.getSpec().getVolumes() != null
                && !pod.getSpec().getVolumes().isEmpty();

        ObjectNode volume = MAPPER.createObjectNode();
        volume.put("name", SIDECAR_VOLUME_NAME);
        ObjectNode downwardAPI = volume.putObject("downwardAPI");
        ArrayNode items = downwardAPI.putArray("items");
        ObjectNode item = items.addObject();
        item.put("path", CONFIG_FILE_NAME);
        ObjectNode fieldRef = item.putObject("fieldRef");
        fieldRef.put("fieldPath", "metadata.annotations['" + Annotations.PROXY_CONFIG + "']");

        if (hasVolumes) {
            addOp(patch, "add", "/spec/volumes/-", volume);
        }
        else {
            ArrayNode volumes = MAPPER.createArrayNode();
            volumes.add(volume);
            addOp(patch, "add", "/spec/volumes", volumes);
        }
    }

    private static void addSidecarContainerOp(
                                              ArrayNode patch,
                                              Pod pod,
                                              String proxyImage,
                                              int managementPort,
                                              KroxyliciousSidecarConfigSpec spec) {

        ObjectNode container = MAPPER.createObjectNode();
        container.put("name", InjectionDecision.SIDECAR_CONTAINER_NAME);
        container.put("image", proxyImage);

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
        addProbe(container, "startupProbe", managementPort, 5, 2, 30);
        addProbe(container, "livenessProbe", managementPort, 30, 10, 3);
        addProbe(container, "readinessProbe", managementPort, 5, 2, 5);

        // Volume mount
        ArrayNode volumeMounts = container.putArray("volumeMounts");
        ObjectNode mount = volumeMounts.addObject();
        mount.put("name", SIDECAR_VOLUME_NAME);
        mount.put("mountPath", CONFIG_MOUNT_PATH);
        mount.put("subPath", CONFIG_FILE_NAME);
        mount.put("readOnly", true);

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

        boolean hasContainers = pod.getSpec() != null
                && pod.getSpec().getContainers() != null
                && !pod.getSpec().getContainers().isEmpty();

        if (hasContainers) {
            addOp(patch, "add", "/spec/containers/-", container);
        }
        else {
            ArrayNode containers = MAPPER.createArrayNode();
            containers.add(container);
            addOp(patch, "add", "/spec/containers", containers);
        }
    }

    private static void addProbe(
                                 ObjectNode container,
                                 String probeName,
                                 int port,
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
            // Don't set on the sidecar itself
            if (InjectionDecision.SIDECAR_CONTAINER_NAME.equals(c.getName())) {
                continue;
            }

            boolean hasEnv = c.getEnv() != null && !c.getEnv().isEmpty();
            ObjectNode envVar = MAPPER.createObjectNode();
            envVar.put("name", KAFKA_BOOTSTRAP_SERVERS_ENV);
            envVar.put("value", bootstrapValue);

            if (hasEnv) {
                // Check if KAFKA_BOOTSTRAP_SERVERS is already set; if so, replace it
                int existingIdx = findEnvVarIndex(c.getEnv(), KAFKA_BOOTSTRAP_SERVERS_ENV);
                if (existingIdx >= 0) {
                    addOp(patch, "replace",
                            "/spec/containers/" + i + "/env/" + existingIdx + "/value",
                            bootstrapValue);
                }
                else {
                    addOp(patch, "add",
                            "/spec/containers/" + i + "/env/-",
                            envVar);
                }
            }
            else {
                ArrayNode envArray = MAPPER.createArrayNode();
                envArray.add(envVar);
                addOp(patch, "add",
                        "/spec/containers/" + i + "/env",
                        envArray);
            }
        }
    }

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
            addOp(patch, "add", "/spec/securityContext", secCtx);
        }
    }

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
