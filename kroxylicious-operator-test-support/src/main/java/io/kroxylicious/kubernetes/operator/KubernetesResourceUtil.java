/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Minimal accessors for Kubernetes resource metadata, avoiding the
 * {@code .getMetadata().getName()} ceremony.
 */
public final class KubernetesResourceUtil {

    private KubernetesResourceUtil() {
    }

    public static String name(HasMetadata resource) {
        return resource.getMetadata().getName();
    }
}
