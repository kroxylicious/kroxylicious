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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpecBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.NodeIdRange;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Handles admission review requests for sidecar injection.
 */
class AdmissionHandler implements HttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdmissionHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String JSON_PATCH_TYPE = "JSONPatch";
    private static final String KROXYLICIOUS_ANNOTATION_PREFIX = "kroxylicious.io/";

    /** Annotation keys that app owners can use to override sidecar config when delegated. */
    static final String DELEGATED_BOOTSTRAP_PORT = "kroxylicious.io/sidecar-bootstrap-port";
    static final String DELEGATED_NODE_ID_RANGE = "kroxylicious.io/sidecar-node-id-range";

    /** Annotations managed by the webhook itself — never treated as undelegated. */
    private static final Set<String> WEBHOOK_MANAGED_ANNOTATIONS = Set.of(
            Annotations.INJECT_SIDECAR,
            Annotations.SIDECAR_CONFIG,
            Annotations.PROXY_CONFIG,
            Annotations.SIDECAR_STATUS);

    private final SidecarConfigResolver configResolver;
    private final String proxyImage;
    private final boolean useNativeSidecar;

    AdmissionHandler(
                     @NonNull SidecarConfigResolver configResolver,
                     @NonNull String proxyImage,
                     boolean useNativeSidecar) {
        this.configResolver = configResolver;
        this.proxyImage = proxyImage;
        this.useNativeSidecar = useNativeSidecar;
    }

    AdmissionHandler(
                     @NonNull SidecarConfigResolver configResolver,
                     @NonNull String proxyImage) {
        this(configResolver, proxyImage, false);
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

            // TODO the below code share a lot of statements in common with sendAllowResponse()
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
            sendAllowResponse(exchange, null);
            // TODO this will need to change when we default to fail closed.
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

            String image = resolveImage(sidecarConfig);
            String jsonPatch = PodMutator.createPatch(pod, effectiveSpec, image, useNativeSidecar);

            AdmissionResponse response = allowResponse(uid);
            response.setPatchType(JSON_PATCH_TYPE);
            response.setPatch(Base64.getEncoder().encodeToString(jsonPatch.getBytes(StandardCharsets.UTF_8)));
            return response;
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .addKeyValue("uid", uid)
                    .log("Error processing admission request, allowing pod without sidecar");
            return allowResponse(uid);
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

        // Warn about undelegated kroxylicious.io/ annotations
        warnAboutUndelegatedAnnotations(namespace, podName, podAnnotations, delegatedSet);

        if (delegatedSet.isEmpty()) {
            return adminSpec;
        }

        // Copy the spec so we don't mutate the cached admin config
        KroxyliciousSidecarConfigSpec effective = copySpec(adminSpec);

        // Apply bootstrap port override
        applyBootstrapPortOverride(podAnnotations, podName, namespace, delegatedSet, effective);

        // Apply node ID range override (format: "start-end")
        applyNodeIdRangeOverride(podAnnotations, podName, namespace, delegatedSet, effective);

        return effective;
    }

    private static void applyNodeIdRangeOverride(Map<String, String> podAnnotations,
                                                 String podName,
                                                 String namespace,
                                                 Set<String> delegatedSet,
                                                 KroxyliciousSidecarConfigSpec effective) {
        if (delegatedSet.contains(DELEGATED_NODE_ID_RANGE)) {
            String rangeStr = podAnnotations.get(DELEGATED_NODE_ID_RANGE);
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
                                .addKeyValue(WebhookLoggingKeys.ANNOTATION, DELEGATED_NODE_ID_RANGE)
                                .addKeyValue(WebhookLoggingKeys.ANNOTATION_VALUE, rangeStr)
                                .log("Invalid node ID range in delegated annotation, using admin default");
                    }
                }
                else {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .addKeyValue(WebhookLoggingKeys.ANNOTATION, DELEGATED_NODE_ID_RANGE)
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
        if (delegatedSet.contains(DELEGATED_BOOTSTRAP_PORT)) {
            String portStr = podAnnotations.get(DELEGATED_BOOTSTRAP_PORT);
            if (portStr != null) {
                try {
                    effective.setBootstrapPort(Long.parseLong(portStr));
                }
                catch (NumberFormatException e) {
                    LOGGER.atWarn()
                            .addKeyValue(WebhookLoggingKeys.POD, podName)
                            .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                            .addKeyValue(WebhookLoggingKeys.ANNOTATION, DELEGATED_NODE_ID_RANGE)
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
            if (key.startsWith(KROXYLICIOUS_ANNOTATION_PREFIX)
                    && !WEBHOOK_MANAGED_ANNOTATIONS.contains(key)
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

    @NonNull
    private String resolveImage(@NonNull KroxyliciousSidecarConfig config) {
        // TODO rename to proxyImage()
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

    private void sendAllowResponse(HttpExchange exchange, String uid) {
        try {
            AdmissionReview responseReview = new AdmissionReview();
            responseReview.setApiVersion("admission.k8s.io/v1");
            responseReview.setKind("AdmissionReview");
            responseReview.setResponse(allowResponse(uid));
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
