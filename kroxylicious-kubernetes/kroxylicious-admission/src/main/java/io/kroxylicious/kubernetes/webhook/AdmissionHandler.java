/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;

import io.kroxylicious.kubernetes.api.admission.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.admission.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.kubernetes.api.admission.v1alpha1.KroxyliciousSidecarConfigSpecBuilder;
import io.kroxylicious.kubernetes.api.admission.v1alpha1.kroxylicioussidecarconfigspec.NodeIdRange;
import io.kroxylicious.kubernetes.api.admission.v1alpha1.kroxylicioussidecarconfigspec.Plugins;
import io.kroxylicious.kubernetes.api.admission.v1alpha1.kroxylicioussidecarconfigspec.plugins.Image;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Handles admission review requests for sidecar injection.
 */
class AdmissionHandler implements HttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdmissionHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String JSON_PATCH_TYPE = "JSONPatch";

    private final SidecarConfigResolver configResolver;
    private final String proxyImage;
    private final boolean useNativeSidecar;
    private final boolean useOciImageVolumes;
    private final boolean failClosed;

    AdmissionHandler(
                     @NonNull SidecarConfigResolver configResolver,
                     @NonNull String proxyImage,
                     KubernetesVersion kubernetesVersion,
                     boolean failClosed) {
        this.configResolver = configResolver;
        this.proxyImage = proxyImage;
        this.useNativeSidecar = kubernetesVersion.supportedNativeSidecar();
        this.useOciImageVolumes = kubernetesVersion.supportsOciImageVolumes();
        this.failClosed = failClosed;
    }

    AdmissionHandler(
                     @NonNull SidecarConfigResolver configResolver,
                     @NonNull String proxyImage) {
        this(configResolver, proxyImage, new KubernetesVersion(1, 0), true);
    }

    @Override
    public void handle(HttpExchange exchange) {
        try {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            AdmissionReview review;
            try (InputStream is = exchange.getRequestBody()) {
                review = MAPPER.readValue(is, AdmissionReview.class);
            }

            AdmissionResponse response = processReview(review);
            AdmissionReview responseReview = new AdmissionReview();
            responseReview.setApiVersion("admission.k8s.io/v1");
            responseReview.setKind("AdmissionReview");
            responseReview.setResponse(response);

            byte[] responseBytes = MAPPER.writeValueAsBytes(responseReview);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .log("Unexpected error handling admission request");
            sendErrorResponse(exchange, null, "Failed to process admission request");
        }
        finally {
            exchange.close();
        }
    }

    @NonNull
    AdmissionResponse processReview(@NonNull AdmissionReview review) {
        AdmissionRequest request = review.getRequest();
        if (request == null) {
            return allowResponse(null);
        }

        String uid = request.getUid();

        try {
            Pod pod = MAPPER.convertValue(request.getObject(), Pod.class);
            if (pod == null) {
                LOGGER.atWarn()
                        .addKeyValue("uid", uid)
                        .log("Admission request contained no pod object");
                return allowResponse(uid);
            }

            String namespace = request.getNamespace();
            String podName = pod.getMetadata() != null ? pod.getMetadata().getName() : null;
            if (podName == null) {
                podName = pod.getMetadata() != null ? pod.getMetadata().getGenerateName() : "<unknown>";
            }

            // Resolve sidecar config
            Map<String, String> annotations = pod.getMetadata() != null ? pod.getMetadata().getAnnotations() : null;
            String explicitConfigName = annotations != null ? annotations.get(Annotations.SIDECAR_CONFIG) : null;
            Optional<KroxyliciousSidecarConfig> configOpt = configResolver.resolve(namespace, explicitConfigName);

            // Evaluate injection decision
            InjectionDecision.Decision decision = InjectionDecision.evaluate(pod, configOpt.isPresent());

            LOGGER.atInfo()
                    .addKeyValue(WebhookLoggingKeys.POD, podName)
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                    .addKeyValue("decision", decision.name())
                    .addKeyValue("sidecarConfig", explicitConfigName)
                    .log("Sidecar injection decision");

            if (decision != InjectionDecision.Decision.INJECT) {
                return allowResponse(uid);
            }

            // Apply delegated annotation overrides
            KroxyliciousSidecarConfig sidecarConfig = configOpt.orElseThrow();
            KroxyliciousSidecarConfigSpec effectiveSpec = applyDelegatedOverrides(
                    sidecarConfig.getSpec(), annotations, podName, namespace);

            String image = proxyImage(sidecarConfig);
            String jsonPatch = PodMutator.createPatch(
                    pod, effectiveSpec, image, useNativeSidecar, useOciImageVolumes);

            AdmissionResponse response = allowResponse(uid);
            response.setPatchType(JSON_PATCH_TYPE);
            response.setPatch(Base64.getEncoder().encodeToString(jsonPatch.getBytes(StandardCharsets.UTF_8)));
            return response;
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .addKeyValue("uid", uid)
                    .log("Error processing admission request");
            return errorResponse(uid, "Error processing admission request: " + e.getMessage());
        }
    }

    /**
     * Applies delegated annotation overrides from the pod to the sidecar config spec.
     * Logs warnings for undelegated annotations in the {@code kroxylicious.io/} namespace.
     */
    @NonNull
    KroxyliciousSidecarConfigSpec applyDelegatedOverrides(
                                                          @NonNull KroxyliciousSidecarConfigSpec adminSpec,
                                                          Map<String, String> podAnnotations,
                                                          String podName,
                                                          String namespace) {

        if (podAnnotations == null || podAnnotations.isEmpty()) {
            return adminSpec;
        }

        List<String> delegated = adminSpec.getDelegatedAnnotations();
        Set<String> delegatedSet = delegated != null ? Set.copyOf(delegated) : Set.of();

        warnAboutUndelegatedAnnotations(namespace, podName, podAnnotations, delegatedSet);

        if (delegatedSet.isEmpty()) {
            return adminSpec;
        }

        // Copy the spec so we don't mutate the cached admin config
        KroxyliciousSidecarConfigSpec effective = copySpec(adminSpec);

        applyBootstrapPortOverride(podAnnotations, podName, namespace, delegatedSet, effective);

        applyNodeIdRangeOverride(podAnnotations, podName, namespace, delegatedSet, effective);

        // Apply delegated plugin images (JSON array of {name, reference} objects)
        applyDelegatedPluginImages(adminSpec, podAnnotations, podName, namespace, delegatedSet, effective);

        return effective;
    }

    private void applyDelegatedPluginImages(@NonNull KroxyliciousSidecarConfigSpec adminSpec, Map<String, String> podAnnotations, String podName, String namespace,
                                            Set<String> delegatedSet, KroxyliciousSidecarConfigSpec effective) {
        if (delegatedSet.contains(Annotations.DELEGATED_PLUGIN_IMAGES)) {
            String pluginsJson = podAnnotations.get(Annotations.DELEGATED_PLUGIN_IMAGES);
            if (pluginsJson != null) {
                List<Plugins> delegatedPlugins = parseDelegatedPlugins(
                        pluginsJson, adminSpec, podName, namespace);
                if (!delegatedPlugins.isEmpty()) {
                    List<Plugins> merged = new java.util.ArrayList<>();
                    if (effective.getPlugins() != null) {
                        merged.addAll(effective.getPlugins());
                    }
                    merged.addAll(delegatedPlugins);
                    effective.setPlugins(merged);
                }
            }
        }
    }

    // TODO: Do we really want to support this for the initial feature?
    private static void applyNodeIdRangeOverride(Map<String, String> podAnnotations,
                                                 String podName,
                                                 String namespace,
                                                 Set<String> delegatedSet,
                                                 KroxyliciousSidecarConfigSpec effective) {
        if (delegatedSet.contains(Annotations.DELEGATED_NODE_ID_RANGE)) {
            String rangeStr = podAnnotations.get(Annotations.DELEGATED_NODE_ID_RANGE);
            if (rangeStr != null) {
                String[] parts = rangeStr.split("-", 2);
                if (parts.length == 2) {
                    try {
                        NodeIdRange range = new NodeIdRange();
                        range.setStartInclusive(Long.parseLong(parts[0]));
                        range.setEndInclusive(Long.parseLong(parts[1]));
                        effective.setNodeIdRange(range);
                    }
                    catch (NumberFormatException e) {
                        LOGGER.atWarn()
                                .addKeyValue(WebhookLoggingKeys.POD, podName)
                                .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                                .addKeyValue(WebhookLoggingKeys.ANNOTATION, Annotations.DELEGATED_NODE_ID_RANGE)
                                .addKeyValue(WebhookLoggingKeys.ANNOTATION_VALUE, rangeStr)
                                .log("Invalid node ID range in delegated annotation, using admin default");
                    }
                }
                else {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .addKeyValue(WebhookLoggingKeys.ANNOTATION, Annotations.DELEGATED_NODE_ID_RANGE)
                            .addKeyValue(WebhookLoggingKeys.ANNOTATION_VALUE, rangeStr)
                            .log("Invalid node ID range format in delegated annotation (expected start-end), using admin default");
                }
            }
        }
    }

    private static void applyBootstrapPortOverride(Map<String, String> podAnnotations,
                                                   String podName,
                                                   String namespace,
                                                   Set<String> delegatedSet,
                                                   KroxyliciousSidecarConfigSpec effective) {
        if (delegatedSet.contains(Annotations.DELEGATED_BOOTSTRAP_PORT)) {
            String portStr = podAnnotations.get(Annotations.DELEGATED_BOOTSTRAP_PORT);
            if (portStr != null) {
                try {
                    effective.setBootstrapPort(Long.parseLong(portStr));
                }
                catch (NumberFormatException e) {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .addKeyValue(WebhookLoggingKeys.ANNOTATION, Annotations.DELEGATED_NODE_ID_RANGE)
                            .addKeyValue(WebhookLoggingKeys.ANNOTATION_VALUE, portStr)
                            .log("Invalid bootstrap port in delegated annotation, using admin default");
                }
            }
        }
    }

    private static void warnAboutUndelegatedAnnotations(String namespace,
                                                        String podName,
                                                        Map<String, String> podAnnotations,
                                                        Set<String> delegatedSet) {
        for (String key : podAnnotations.keySet()) {
            if (Annotations.isKroxyliciousAnnotation(key)
                    && !Annotations.isWebhookManagedAnnotation(key)
                    && !delegatedSet.contains(key)) {
                LOGGER.atWarn()
                        .addKeyValue(WebhookLoggingKeys.POD, podName)
                        .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                        .addKeyValue("annotation", key)
                        .log("Pod has undelegated kroxylicious.io annotation, ignoring");
            }
        }
    }

    @NonNull
    private static KroxyliciousSidecarConfigSpec copySpec(@NonNull KroxyliciousSidecarConfigSpec src) {
        KroxyliciousSidecarConfigSpecBuilder builder = new KroxyliciousSidecarConfigSpecBuilder(src);
        return builder.build();
    }

    /**
     * Parses and validates delegated plugin images from a pod annotation.
     * Rejects images that do not include a {@code @sha256:} digest or that do not match
     * one of the allowed registry prefixes.
     */
    @SuppressWarnings("S135") // for loop with > 1 continue, but refactoring would be header to understand
    @NonNull
    List<Plugins> parseDelegatedPlugins(
                                        @NonNull String pluginsJson,
                                        @NonNull KroxyliciousSidecarConfigSpec adminSpec,
                                        String podName,
                                        String namespace) {

        List<Plugins> result = new java.util.ArrayList<>();
        List<String> allowed = adminSpec.getAllowedPluginRegistries();
        Set<String> allowedSet = allowed != null ? Set.copyOf(allowed) : Set.of();

        try {
            JsonNode array = MAPPER.readTree(pluginsJson);
            if (!array.isArray()) {
                LOGGER.atWarn()
                        .addKeyValue(WebhookLoggingKeys.POD, podName)
                        .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                        .log("Delegated plugin images annotation is not a JSON array, ignoring");
                return result;
            }

            for (JsonNode entry : array) {
                String name = entry.path("name").asText(null);
                String reference = entry.path("reference").asText(null);
                if (name == null || reference == null) {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .log("Delegated plugin entry missing name or reference, skipping");
                    continue;
                }

                // Require @sha256: digest
                if (!reference.contains("@sha256:")) {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .addKeyValue(WebhookLoggingKeys.IMG_REF, reference)
                            .log("Delegated plugin image rejected: must include @sha256: digest");
                    continue;
                }

                // Validate against allowed registries
                if (!allowedSet.isEmpty()
                        && allowedSet.stream().noneMatch(reference::startsWith)) {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .addKeyValue(WebhookLoggingKeys.IMG_REF, reference)
                            .log("Delegated plugin image rejected: registry not in allowedPluginRegistries");
                    continue;
                }

                LOGGER.atInfo()
                        .addKeyValue(WebhookLoggingKeys.POD, podName)
                        .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                        .addKeyValue("plugin", name)
                        .addKeyValue(WebhookLoggingKeys.IMG_REF, reference)
                        .log("Accepting delegated plugin image");

                Plugins plugin = new Plugins();
                plugin.setName(name);
                Image image = new Image();
                image.setReference(reference);
                plugin.setImage(image);
                result.add(plugin);
            }
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            LOGGER.atWarn()
                    .addKeyValue(WebhookLoggingKeys.POD, podName)
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                    .addKeyValue(WebhookLoggingKeys.ERROR, e.getMessage())
                    .log("Failed to parse delegated plugin images JSON, ignoring");
        }

        return result;
    }

    @NonNull
    private String proxyImage(@NonNull KroxyliciousSidecarConfig config) {
        String image = config.getSpec().getProxyImage();
        return image != null ? image : proxyImage;
    }

    @NonNull
    private static AdmissionResponse allowResponse(String uid) {
        AdmissionResponse response = new AdmissionResponse();
        response.setUid(uid != null ? uid : UUID.randomUUID().toString());
        response.setAllowed(true);
        return response;
    }

    @NonNull
    private static AdmissionResponse denyResponse(String uid, String reason) {
        AdmissionResponse response = new AdmissionResponse();
        response.setUid(uid != null ? uid : UUID.randomUUID().toString());
        response.setAllowed(false);
        Status status = new Status();
        status.setCode(403);
        status.setMessage(reason);
        status.setReason("Forbidden");
        status.setStatus("Failure");
        response.setStatus(status);
        return response;
    }

    @NonNull
    private AdmissionResponse errorResponse(String uid, String reason) {
        if (failClosed) {
            return denyResponse(uid, reason);
        }
        return allowResponse(uid);
    }

    private void sendErrorResponse(HttpExchange exchange, String uid, String reason) {
        try {
            AdmissionReview responseReview = new AdmissionReview();
            responseReview.setApiVersion("admission.k8s.io/v1");
            responseReview.setKind("AdmissionReview");
            responseReview.setResponse(errorResponse(uid, reason));
            byte[] responseBytes = MAPPER.writeValueAsBytes(responseReview);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
        catch (IOException e) {
            LOGGER.atError()
                    .setCause(e)
                    .log("Failed to send error response");
        }
    }
}
