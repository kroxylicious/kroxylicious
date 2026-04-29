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
    static final String SIDECAR_INJECTION = "kroxylicious.io/sidecar-injection";
    
    /**
     * The value of the {@code kroxylicious.io/sidecar-injection} label which enables
     * interception by the webhook.
     */
    static final String SIDECAR_INJECTION_ENABLED = "enabled";

    private Labels() {
    }
}
