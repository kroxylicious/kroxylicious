/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static org.assertj.core.api.Assertions.assertThat;

class PodIdentityCredentialsProviderConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(YAMLFactory.builder().build());

    @Test
    void deserializesAllFields() {
        var yaml = """
                credentialsFullUri: http://169.254.170.23/v1/credentials
                authorizationTokenFile: /var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token
                credentialLifetimeFactor: 0.5
                """;

        var config = readConfig(yaml);

        assertThat(config.credentialsFullUri()).isEqualTo(URI.create("http://169.254.170.23/v1/credentials"));
        assertThat(config.authorizationTokenFile()).isEqualTo(Path.of("/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token"));
        assertThat(config.credentialLifetimeFactor()).isEqualTo(0.5);
    }

    @Test
    void allFieldsOptional() {
        var config = readConfig("{}");

        assertThat(config.credentialsFullUri()).isNull();
        assertThat(config.authorizationTokenFile()).isNull();
        assertThat(config.credentialLifetimeFactor()).isNull();
    }

    private PodIdentityCredentialsProviderConfig readConfig(String yaml) {
        try {
            return OBJECT_MAPPER.readValue(yaml, PodIdentityCredentialsProviderConfig.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
