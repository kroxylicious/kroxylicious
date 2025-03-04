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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecureConfigInterpolatorTest {

    private static final YAMLMapper YAML_MAPPER = new YAMLMapper()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
            .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR);

    @Test
    void shouldInterpolateInAnArray() throws JsonProcessingException {
        // given
        Map<String, SecureConfigProvider> secret = Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER);
        var i = new SecureConfigInterpolator("/base", secret);

        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                array:
                  - 1
                  - ${secret:aws:access-key}
                  - true
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).singleElement().extracting(Volume::getName).isEqualTo("secrets-aws");
        assertThat(result.volumes()).singleElement().extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("aws");

        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getName).isEqualTo("secrets-aws");
        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/aws");

        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                array:
                  - 1
                  - /base/secret/aws/access-key
                  - true
                """);

    }

    @Test
    void shouldInterpolateInAnObject() throws JsonProcessingException {
        // given
        Map<String, SecureConfigProvider> secret = Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER);
        var i = new SecureConfigInterpolator("/base", secret);

        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                object:
                  one: 1
                  accessKey: ${secret:aws:access-key}
                  yarp: true
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).singleElement().extracting(Volume::getName).isEqualTo("secrets-aws");
        assertThat(result.volumes()).singleElement().extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("aws");

        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getName).isEqualTo("secrets-aws");
        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/aws");

        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                object:
                  one: 1
                  accessKey: /base/secret/aws/access-key
                  yarp: true
                """);
    }

    @Test
    void shouldInterpolateTopLevelString() throws JsonProcessingException {
        // given
        Map<String, SecureConfigProvider> secret = Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER);
        var i = new SecureConfigInterpolator("/base", secret);

        var jsonValue = YAML_MAPPER.readValue("\"${secret:aws:access-key}\"", String.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).singleElement().extracting(Volume::getName).isEqualTo("secrets-aws");
        assertThat(result.volumes()).singleElement().extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("aws");

        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getName).isEqualTo("secrets-aws");
        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/aws");

        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("/base/secret/aws/access-key\n");
    }

    @Test
    void shouldNotTnterpolateWhenQuoted() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));
        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                kmsConfig:
                  quoted:              \\${secret:different-secret:a-key}
                  notQuoted:         \\\\${secret:different-secret:a-key}
                  alsoQuoted:      \\\\\\${secret:different-secret:a-key}
                  alsoNotQuoted: \\\\\\\\${secret:different-secret:a-key}
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).singleElement().extracting(Volume::getName).isEqualTo("secrets-different-secret");
        assertThat(result.volumes()).singleElement().extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("different-secret");

        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getName).isEqualTo("secrets-different-secret");
        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/different-secret");

        // Note that in YAML itself \ functions as an escape character, but only in double-quoted strings.
        // Then we have to quote \ for Java source code
        // So in the below `notQuoted` and `alsoQuoted` only feature a single \ in YAML-space
        // and `alsoNotQuoted` features a two \ in YAML-space.
        assertThat(new YAMLMapper(YAML_MAPPER).disable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                .writeValueAsString(result.config())).isEqualTo("""
                kms: "AwsKms"
                kmsConfig:
                  quoted: "${secret:different-secret:a-key}"
                  notQuoted: "\\\\/base/secret/different-secret/a-key"
                  alsoQuoted: "\\\\${secret:different-secret:a-key}"
                  alsoNotQuoted: "\\\\\\\\/base/secret/different-secret/a-key"
                """);

    }

    @Test
    void shouldNotTnterpolateWhenPrefixed() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));
        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                kmsConfig:
                  prefixed:
                    prefix ${secret:different-secret:a-key}
                  suffixed:
                    ${secret:different-secret:a-key} suffix
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).isEmpty();
        assertThat(result.mounts()).isEmpty();
        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                kmsConfig:
                  prefixed: "prefix ${secret:different-secret:a-key}"
                  suffixed: "${secret:different-secret:a-key} suffix"
                """);

    }

    @Test
    void shouldThrowFromInterpolateWhenUnknownProvider() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));
        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                kmsConfig:
                  knownProvider: ${secret:aws:a-key}
                  unknownProvider: ${unknow:aws:a-key}
                """, Map.class);

        // then
        assertThatThrownBy(() -> i.interpolate(jsonValue))
                .isInstanceOf(InterpolationException.class)
                .hasMessage("Unknown config provider 'unknow', known providers are: [secret]");

    }

    @Test
    void shouldGetOneVolumeAndMountForMultipleIdenticalPlaceholders() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));
        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: ${secret:aws:access-key}
                  alsoAccessKey:
                    password: ${secret:aws:access-key}
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).singleElement().extracting(Volume::getName).isEqualTo("secrets-aws");
        assertThat(result.volumes()).singleElement().extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("aws");

        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getName).isEqualTo("secrets-aws");
        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/aws");

        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: /base/secret/aws/access-key
                  alsoAccessKey:
                    password: /base/secret/aws/access-key
                """);
    }

    @Test
    void shouldGetOneVolumeAndMountForPlaceholdersWithDifferentKeys() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));
        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: ${secret:aws:access-key}
                  secretKey:
                    password: ${secret:aws:secret-key}
                """, Map.class);

        // when
        var result = i.interpolate(jsonValue);

        // then
        assertThat(result.volumes()).singleElement().extracting(Volume::getName).isEqualTo("secrets-aws");
        assertThat(result.volumes()).singleElement().extracting(Volume::getSecret).extracting(SecretVolumeSource::getSecretName).isEqualTo("aws");

        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getName).isEqualTo("secrets-aws");
        assertThat(result.mounts()).singleElement().extracting(VolumeMount::getMountPath).isEqualTo("/base/secret/aws");

        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: /base/secret/aws/access-key
                  secretKey:
                    password: /base/secret/aws/secret-key
                """);
    }

    @Test
    void shouldGetTwoVolumesAndMountsForTwoSecretes() throws JsonProcessingException {
        // given
        var i = new SecureConfigInterpolator("/base", Map.of("secret", MountedResourceConfigProvider.SECRET_PROVIDER));

        var jsonValue = YAML_MAPPER.readValue("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: ${secret:aws:access-key}
                  anotherKey:
                    password: ${secret:different-secret:access-key}
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

        assertThat(YAML_MAPPER.writeValueAsString(result.config())).isEqualTo("""
                kms: AwsKms
                kmsConfig:
                  accessKey:
                    password: /base/secret/aws/access-key
                  anotherKey:
                    password: /base/secret/different-secret/access-key
                """);

    }

}
