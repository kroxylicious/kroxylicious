/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

/**
 * Annotation keys used by the sidecar injection webhook.
 */
final class Annotations {

    // TODO Javadoc these things, so we know what they're for
    static final String INJECT_SIDECAR = "kroxylicious.io/inject-sidecar";
    static final String SIDECAR_CONFIG = "kroxylicious.io/sidecar-config";
    static final String PROXY_CONFIG = "kroxylicious.io/proxy-config";
    static final String SIDECAR_STATUS = "kroxylicious.io/sidecar-status";

    private Annotations() {
    }
}
