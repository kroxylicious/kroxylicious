/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

/**
 * Label keys used by the sidecar injection webhook.
 */
final class Labels {

    /**
     * Label applied to Kubernetes {@code Namespace} resources controlling whether
     * {@code Pods} created in that {@code Namespace} will be intercepted by the webhook.
     * Interception is enabled when this label has the value {@code enabled}.
     */
    static final String SIDECAR_INJECTION = "sidecar.kroxylicious.io/injection";

    /**
     * The value of the {@code sidecar.kroxylicious.io/injection} label which enables
     * interception by the webhook.
     */
    static final String SIDECAR_INJECTION_ENABLED = "enabled";

    /**
     * The value of the {@code sidecar.kroxylicious.io/injection} label on a pod
     * which disables sidecar injection for that pod.
     * Also used as an {@code objectSelector} on the {@code MutatingWebhookConfiguration}
     * so that pods with this value are never sent to the webhook.
     */
    static final String SIDECAR_INJECTION_DISABLED = "disabled";

    /**
     * Label set on pods where sidecar injection was skipped for a reason
     * other than opt-out. The value indicates the reason (e.g. {@code no-config},
     * {@code already-injected}).
     */
    static final String INJECTION_SKIPPED = "sidecar.kroxylicious.io/injection-skipped";

    private Labels() {
    }
}
