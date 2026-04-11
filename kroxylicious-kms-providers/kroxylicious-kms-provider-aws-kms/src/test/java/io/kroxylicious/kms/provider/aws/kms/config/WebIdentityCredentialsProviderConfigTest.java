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

class WebIdentityCredentialsProviderConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(YAMLFactory.builder().build());

    @Test
    void deserializesAllFields() {
        var yaml = """
                roleArn: arn:aws:iam::123456789012:role/MyRole
                webIdentityTokenFile: /var/run/secrets/eks.amazonaws.com/serviceaccount/token
                roleSessionName: my-session
                stsEndpointUrl: https://sts.us-east-1.amazonaws.com
                stsRegion: us-east-1
                durationSeconds: 3600
                credentialLifetimeFactor: 0.5
                """;

        var config = readConfig(yaml);

        assertThat(config.roleArn()).isEqualTo("arn:aws:iam::123456789012:role/MyRole");
        assertThat(config.webIdentityTokenFile()).isEqualTo(Path.of("/var/run/secrets/eks.amazonaws.com/serviceaccount/token"));
        assertThat(config.roleSessionName()).isEqualTo("my-session");
        assertThat(config.stsEndpointUrl()).isEqualTo(URI.create("https://sts.us-east-1.amazonaws.com"));
        assertThat(config.stsRegion()).isEqualTo("us-east-1");
        assertThat(config.durationSeconds()).isEqualTo(3600);
        assertThat(config.credentialLifetimeFactor()).isEqualTo(0.5);
    }

    @Test
    void allFieldsOptional() {
        var config = readConfig("{}");

        assertThat(config.roleArn()).isNull();
        assertThat(config.webIdentityTokenFile()).isNull();
        assertThat(config.roleSessionName()).isNull();
        assertThat(config.stsEndpointUrl()).isNull();
        assertThat(config.stsRegion()).isNull();
        assertThat(config.durationSeconds()).isNull();
        assertThat(config.credentialLifetimeFactor()).isNull();
    }

    private WebIdentityCredentialsProviderConfig readConfig(String yaml) {
        try {
            return OBJECT_MAPPER.readValue(yaml, WebIdentityCredentialsProviderConfig.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
