/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

/**
 * Provides manifests from single YAML files.
 * Supports the GitOps approach where CRDs and installation manifests are separate single files.
 */
public class DirectManifestProvider implements ManifestProvider {

    private final File crdYaml;
    private final File installYaml;

    public DirectManifestProvider(Path crdYaml, Path installYaml) {
        this.crdYaml = crdYaml.toFile();
        this.installYaml = installYaml.toFile();
    }

    @Override
    public List<File> getCrdYamls() {
        return List.of(crdYaml);
    }

    @Override
    public List<File> getInstallYamls() {
        return List.of(installYaml);
    }
}
