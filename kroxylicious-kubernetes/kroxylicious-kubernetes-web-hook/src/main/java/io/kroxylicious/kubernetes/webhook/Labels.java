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

    // TODO Javadoc these things, so we know what they're for
    static final String SIDECAR_INJECTION = "kroxylicious.io/sidecar-injection";
    static final String SIDECAR_INJECTION_ENABLED = "enabled";

    private Labels() {
    }
}
