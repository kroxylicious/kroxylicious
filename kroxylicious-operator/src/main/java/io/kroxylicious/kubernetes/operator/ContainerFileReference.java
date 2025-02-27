/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import java.nio.file.Path;
import java.util.Objects;

/**
 * A reference to a file in a container.
 * @param volume The volume on which the file will exist
 * @param mount The volume mount beneath which the file will exist
 * @param containerPath The absolute path of the file
 */
public record ContainerFileReference(
        @Nullable Volume volume,
        @Nullable VolumeMount mount,
        @NonNull Path containerPath) {
    public ContainerFileReference {
        Objects.requireNonNull(containerPath);
        if (volume != null) {
            if (mount == null) {
                throw new IllegalArgumentException("volume and mount must both be non-null, or must both be null");
            }
            if (volume.getName().trim().isEmpty()) {
                throw new IllegalArgumentException("volume name cannot be empty");
            }
            if (!Objects.equals(volume.getName(), mount.getName())) {
                throw new IllegalArgumentException("volume and mount must have the same name");
            }
            if (!containerPath.isAbsolute()) {
                throw new IllegalArgumentException("containerPath must be absolute");
            }
            if (mount.getMountPath() == null || mount.getMountPath().trim().isEmpty()) {
                throw new IllegalArgumentException("mount path cannot be null or empty");
            }
            if (!containerPath.startsWith(Path.of(mount.getMountPath()))) {
                throw new IllegalArgumentException("mount path is not a prefix of the returned container path");
            }
        }
        else if (mount != null) {
            throw new IllegalArgumentException("volume and mount must both be non-null, or must both be null");
        }
    }
}
