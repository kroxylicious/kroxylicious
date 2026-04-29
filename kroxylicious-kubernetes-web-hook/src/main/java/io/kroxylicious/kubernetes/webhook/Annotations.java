/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.Set;

/**
 * Annotation keys used by the sidecar injection webhook.
 */
final class Annotations {

    private static final String KROXYLICIOUS_ANNOTATION_PREFIX = "kroxylicious.io/";

    // TODO Javadoc these things, so we know what they're for
    /**
     * Annotation on the target Pod, when the value is false the sidecar will not be injected into the Pod
     */
    static final String INJECT_SIDECAR = "kroxylicious.io/inject-sidecar";

    /**
     * Annotation on the target Pod used to name the {@code KroxyliciousSidecarConfig}
     * resource which should apply to that Pod.
     */
    static final String SIDECAR_CONFIG = "kroxylicious.io/sidecar-config";

    /**
     * Annotation on the target Pod used to store the proxy configuration, consumed
     * by the sidecar container via a downwardAPI volume mount.
     */
    static final String PROXY_CONFIG = "kroxylicious.io/proxy-config";

    /**
     * Annotation on the target Pod which takes the value {@code "injected"} when
     * the webhook has mutated the pod spec (used because the
     * {@ocde MutatingWebhookConfiguration} is configured with
     * {@code reinvocationPolicy: IfNeeded}).
     */
    static final String SIDECAR_STATUS = "kroxylicious.io/sidecar-status";

    /**
     * <p>Annotation that app owners may set on the {@code Pod}
     * to configure the port used in the value of the
     * {@code KAFKA_BOOTSTRAP_SERVERS} which is set on the all the containers
     * in the target {@code Pod} except the proxy sidecar container.</p>
     *
     * <p>The ability of the app owner to use this annotation
     * depends on whether this annotation has been delegatated to them in the
     * {@ocde MutatingWebhookConfiguration}</p>
     */
    static final String DELEGATED_BOOTSTRAP_PORT = "kroxylicious.io/sidecar-bootstrap-port";

    /**
     * <p>Annotation that app owners may set on the target {@code Pod} to configure
     * node id range which will be used in the proxy configuration
     * consumed by the proxy sidecar.</p>
     *
     * <p>The ability of the app owner to use this annotation
     * depends on whether this annotation has been delegatated to them in the
     * {@ocde MutatingWebhookConfiguration}</p>
     */
    static final String DELEGATED_NODE_ID_RANGE = "kroxylicious.io/sidecar-node-id-range";

    /** The set of annotations managed by the webhook itself — never treated as undelegated. */
    static final Set<String> WEBHOOK_MANAGED_ANNOTATIONS = Set.of(
            INJECT_SIDECAR,
            SIDECAR_CONFIG,
            PROXY_CONFIG,
            SIDECAR_STATUS);

    /**
     * Determines whether the given annotation is one that's managed by the webhook itself
     */
    static boolean isWebhookManagedAnnotation(String key) {
        return WEBHOOK_MANAGED_ANNOTATIONS.contains(key);
    }

    static boolean isKroxyliciousAnnotation(String key) {
        return key.startsWith(KROXYLICIOUS_ANNOTATION_PREFIX);
    }

    private Annotations() {
    }
}
