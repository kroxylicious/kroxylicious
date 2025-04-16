/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.function.BiFunction;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

/**
 * A {@link SecureConfigProvider} for Kubernetes Secrets and ConfigMaps
 */
public class MountedResourceConfigProvider implements SecureConfigProvider {

    static final MountedResourceConfigProvider SECRET_PROVIDER = new MountedResourceConfigProvider("", "secrets",
            (vb, resourceName) -> vb.withNewSecret().withSecretName(resourceName).endSecret());
    static final MountedResourceConfigProvider CONFIGMAP_PROVIDER = new MountedResourceConfigProvider("", "configmaps",
            (vb, resourceName) -> vb.withNewConfigMap().withName(resourceName).endConfigMap());

    private final String group;
    private final String plural;
    private final BiFunction<VolumeBuilder, String, VolumeBuilder> volumeBuilder;

    MountedResourceConfigProvider(String group,
                                  String plural,
                                  BiFunction<VolumeBuilder, String, VolumeBuilder> volumeBuilder) {
        this.group = group;
        this.plural = plural;
        this.volumeBuilder = volumeBuilder;
    }

    @Override
    public ContainerFileReference containerFile(
                                                String providerName,
                                                String resourceName,
                                                String key,
                                                Path mountPathBase) {
        try {
            String volumeName = ResourcesUtil.volumeName(group, plural, resourceName);
            Path mountPath = mountPathBase.resolve(providerName).resolve(resourceName);
            Path itemPath = mountPath.resolve(key);
            Volume volume = volumeBuilder.apply(new VolumeBuilder(), resourceName)
                    .withName(volumeName)
                    .build();
            VolumeMount mount = new VolumeMountBuilder()
                    .withName(volumeName)
                    .withMountPath(mountPath.toString())
                    .withReadOnly(true)
                    .build();
            return new ContainerFileReference(
                    volume,
                    mount,
                    itemPath);
        }
        catch (IllegalArgumentException e) {
            throw new InterpolationException("Cannot construct mounted volume for ${%s:%s:%s}".formatted(
                    providerName,
                    resourceName,
                    key,
                    e));
        }
    }
}
