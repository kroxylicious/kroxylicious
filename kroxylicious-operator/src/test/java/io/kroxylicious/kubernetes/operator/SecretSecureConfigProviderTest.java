/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import static org.assertj.core.api.Assertions.assertThat;

class SecretSecureConfigProviderTest {

    @Test
    void shouldReturnWellFormedFileReference() {
        // given
        MountedResourceConfigProvider mountedResourceConfigProvider = MountedResourceConfigProvider.SECRET_PROVIDER;

        // when
        var cp = mountedResourceConfigProvider.containerFile("secret1", "my-secret", "tls.key", Path.of("/prefix"));

        // then
        assertThat(cp.volume()).extracting(Volume::getName).isEqualTo("secrets-my-secret");
        assertThat(cp.volume()).extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("my-secret");

        assertThat(cp.mount()).extracting(VolumeMount::getName).isEqualTo("secrets-my-secret");
        assertThat(cp.mount()).extracting(VolumeMount::getMountPath).isEqualTo("/prefix/secret1/my-secret");

        assertThat(cp.containerPath()).extracting(Path::toString).isEqualTo("/prefix/secret1/my-secret/tls.key");
    }
}
