/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.util.List;

/**
 * Provides manifests for Kroxylicious operator installation.
 * Supports different sources: archive-extracted manifests or direct single-file YAML manifests.
 */
public interface ManifestProvider {

    /**
     * Get YAML files containing CustomResourceDefinitions.
     * @return list of CRD YAML files
     */
    List<File> getCrdYamls();

    /**
     * Get YAML files for installation (namespace, RBAC, deployment, services, etc).
     * @return list of install YAML files
     */
    List<File> getInstallYamls();
}
