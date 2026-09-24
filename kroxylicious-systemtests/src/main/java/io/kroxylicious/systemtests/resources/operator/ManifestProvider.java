/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Provides Kubernetes resources for installation.
 * Supports different sources: archive-extracted manifests or direct single-file YAML manifests.
 */
public interface ManifestProvider {

    /**
     * Get all Kubernetes resources to apply for installation.
     * Includes CustomResourceDefinitions, namespace, RBAC, deployment, services, etc.
     * @return list of Kubernetes resources in order
     */
    List<HasMetadata> getResources();
}
