/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import static org.assertj.core.api.Assertions.assertThat;

class SecureConfigInterpolatorTest {

    @Test
    void shouldInterpolate() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));
        YAMLMapper yamlMapper = new YAMLMapper()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
        var jsonValue = yamlMapper.readValue("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: ${secret:aws:access-key}
                  secretKey:
                    password: ${secret:aws:secret-key}
                  anotherKey:
                    password: ${secret:different-secret:a-key}
                  notInterpolated:
                    - ${unknown:different-secret:a-key}
                    - prefixed ${secret:different-secret:a-key}
                    - ${secret:different-secret:a-key} suffixed
                array:
                  - ${secret:aws:access-key}
                  - ${secret:different-secret:a-key}
                  - 1
                  - true
                  - null
                  - 3.141
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).hasSize(2);
        assertThat(result.volumes()).element(0).extracting(Volume::getName).isEqualTo("secrets-aws");
        assertThat(result.volumes()).element(0).extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("aws");

        assertThat(result.volumes()).element(1).extracting(Volume::getName).isEqualTo("secrets-different-secret");
        assertThat(result.volumes()).element(1).extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("different-secret");

        assertThat(result.mounts()).hasSize(2);
        assertThat(result.mounts()).element(0).extracting(VolumeMount::getName).isEqualTo("secrets-aws");
        assertThat(result.mounts()).element(0).extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/aws");
        assertThat(result.mounts()).element(1).extracting(VolumeMount::getName).isEqualTo("secrets-different-secret");
        assertThat(result.mounts()).element(1).extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/different-secret");
        assertThat(yamlMapper.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: /base/secret/aws/access-key
                  secretKey:
                    password: /base/secret/aws/secret-key
                  anotherKey:
                    password: /base/secret/different-secret/a-key
                  notInterpolated:
                  - "${unknown:different-secret:a-key}"
                  - "prefixed ${secret:different-secret:a-key}"
                  - "${secret:different-secret:a-key} suffixed"
                array:
                - /base/secret/aws/access-key
                - /base/secret/different-secret/a-key
                - 1
                - true
                - null
                - 3.141
                """);

    }

}
