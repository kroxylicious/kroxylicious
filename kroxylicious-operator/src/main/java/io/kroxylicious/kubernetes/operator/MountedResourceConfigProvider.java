/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.function.BiFunction;

import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

/**
 * A {@link SecureConfigProvider} for Kubernetes Secrets.
 */
public class MountedResourceConfigProvider implements SecureConfigProvider {

    static final MountedResourceConfigProvider SECRET_PROVIDER = new MountedResourceConfigProvider("Secret",
            (vb, resourceName) -> vb.withNewSecret().withSecretName(resourceName).endSecret());
    static final MountedResourceConfigProvider CONFIGMAP_PROVIDER = new MountedResourceConfigProvider("ConfigMap",
            (vb, resourceName) -> vb.withNewConfigMap().withName(resourceName).endConfigMap());

    private final String kind;
    private final BiFunction<VolumeBuilder, String, VolumeBuilder> volumeBuilder;

    MountedResourceConfigProvider(String kind, BiFunction<VolumeBuilder, String, VolumeBuilder> volumeBuilder) {
        this.kind = kind;
        this.volumeBuilder = volumeBuilder;
    }

    @Override
    public ContainerFileReference containerFile(
            String providerName,
            String resourceName,
            String key,
            Path mountPathBase
    ) {
        // TODO validate the secretName and key
        String volumeName = kind + "-" + resourceName;
        Path mountPath = mountPathBase.resolve(providerName).resolve(resourceName);
        Path itemPath = mountPath.resolve(key);
        return new ContainerFileReference(
                volumeBuilder.apply(new VolumeBuilder(), resourceName)
                        .withName(volumeName)
                        .build(),
                new VolumeMountBuilder()
                        .withName(volumeName)
                        .withMountPath(mountPath.toString())
                        .build(),
                itemPath
        );
    }
}
