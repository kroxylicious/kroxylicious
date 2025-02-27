/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A reference to a file in a container, which may be on a mounted volume.
 */
public record ContainerFileReference(
                                     @Nullable Volume volume,
                                     @Nullable VolumeMount mount,
                                     @NonNull Path containerPath) {

    /**
     * A {@code volume} and {@code mount} must either both be non-null, or both be null.
     * When they're both present they must have the same name.
     * @param volume The volume on which the file will exist. Nullable.
     * @param mount The volume mount beneath which the file will exist. Nullable.
     * @param containerPath The absolute path of the file.
     * May not contain any {@code ..} components. If a {@code mount} is given this path must begin with the mount path.
     * @throws IllegalArgumentException If any arguments are not valid
     */
    public ContainerFileReference {
        Objects.requireNonNull(containerPath);
        if (!containerPath.isAbsolute()) {
            throw new IllegalArgumentException("container path must be absolute");
        }
        for (int i = 0; i < containerPath.getNameCount(); i++) {
            if ("..".equals(containerPath.getName(i).toString())) {
                throw new IllegalArgumentException("container path cannot contain a '..' path component");
            }
        }
        if (volume != null) {
            if (mount == null) {
                throw new IllegalArgumentException("volume and mount must both be non-null, or must both be null");
            }
            ResourcesUtil.requireIsDnsLabel(volume.getName(), true, "volume name is not a DNS label");
            if (!Objects.equals(volume.getName(), mount.getName())) {
                throw new IllegalArgumentException("volume and mount must have the same name");
            }
            if (mount.getMountPath() == null || mount.getMountPath().trim().isEmpty()) {
                throw new IllegalArgumentException("mount path cannot be null or empty");
            }
            if (mount.getMountPath().indexOf(':') >= 0) {
                throw new IllegalArgumentException("mount path cannot contain ':'");
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
