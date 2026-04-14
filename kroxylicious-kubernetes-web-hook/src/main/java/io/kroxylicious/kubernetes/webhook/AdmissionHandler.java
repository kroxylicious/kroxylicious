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
import java.util.Map;
import java.util.Optional;
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

    AdmissionHandler(
                     @NonNull SidecarConfigResolver configResolver,
                     @NonNull String proxyImage) {
        this.configResolver = configResolver;
        this.proxyImage = proxyImage;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
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
            // Fail-open: always allow, even on error
            sendAllowResponse(exchange, null);
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
                    .addKeyValue("pod", podName)
                    .addKeyValue("namespace", namespace)
                    .addKeyValue("decision", decision.name())
                    .addKeyValue("sidecarConfig", explicitConfigName)
                    .log("Sidecar injection decision");

            if (decision != InjectionDecision.Decision.INJECT) {
                return allowResponse(uid);
            }

            // Generate the patch
            KroxyliciousSidecarConfig sidecarConfig = configOpt.get();
            String image = resolveImage(sidecarConfig);
            String jsonPatch = PodMutator.createPatch(pod, sidecarConfig.getSpec(), image);

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

    @NonNull
    private String resolveImage(@NonNull KroxyliciousSidecarConfig config) {
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
