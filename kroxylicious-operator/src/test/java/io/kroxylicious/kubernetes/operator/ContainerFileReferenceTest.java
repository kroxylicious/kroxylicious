/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContainerFileReferenceTest {

    @Test
    void shouldValidateConstructorArguments() {
        // given
        Volume volume = new VolumeBuilder().withName("foo").build();
        Volume volumeEmptyName = new VolumeBuilder().withName("").build();
        VolumeMount barMount = new VolumeMountBuilder().withName("bar").build();
        VolumeMount mount = new VolumeMountBuilder().withName("foo").withMountPath("/foo").build();
        VolumeMount badMountPath = new VolumeMountBuilder().withName("foo").withMountPath("/fudge").build();
        VolumeMount emptyMountPath = new VolumeMountBuilder().withName("foo").withMountPath("").build();
        VolumeMount colonContainingMountPath = new VolumeMountBuilder().withName("foo").withMountPath("/f:oo").build();
        Path containerPath = Path.of("/foo/quux");
        Path relativePath = Path.of("foo");

        // then
        assertThatThrownBy(() -> new ContainerFileReference(volume, null, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("volume and mount must both be non-null, or must both be null");
        assertThatThrownBy(() -> new ContainerFileReference(null, mount, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("volume and mount must both be non-null, or must both be null");
        assertThatThrownBy(() -> new ContainerFileReference(volumeEmptyName, mount, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("volume name is not a dns label");
        assertThatThrownBy(() -> new ContainerFileReference(volume, barMount, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("volume and mount must have the same name");
        assertThatThrownBy(() -> new ContainerFileReference(volume, mount, relativePath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("containerPath must be absolute");
        assertThatThrownBy(() -> new ContainerFileReference(volume, emptyMountPath, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("mount path cannot be null or empty");
        assertThatThrownBy(() -> new ContainerFileReference(volume, colonContainingMountPath, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("mount path cannot contain ':'");
        assertThatThrownBy(() -> new ContainerFileReference(volume, badMountPath, containerPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("mount path is not a prefix of the returned container path");

        assertThatNoException().isThrownBy(() -> new ContainerFileReference(volume, mount, containerPath));
        assertThatNoException().isThrownBy(() -> new ContainerFileReference(null, null, containerPath));

    }

}
