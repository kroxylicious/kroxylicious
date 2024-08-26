/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.aws.kms.credentials.Credentials;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigParseTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void fixedCredentials() throws IOException {
        String json = """
                {
                    "endpointUrl": "https://kms.us-east-1.amazon.com",
                    "credentialsProvider": {
                        "type": "fixed",
                        "accessKeyId": {
                            "password": "myAccessKeyId"
                        },
                        "secretAccessKey": {
                            "password": "mySecretAccessKey"
                        }
                    },
                    "region": "us-east-1"
                }
                """;
        Config config = readConfig(json);
        assertThat(config.endpointUrl())
                .extracting(URI::toString)
                .isEqualTo("https://kms.us-east-1.amazon.com");
        assertThat(config.region())
                .isEqualTo("us-east-1");
        try (var provider = config.credentialsProvider().createCredentialsProvider()) {
            assertThat(provider.getCredentials())
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isEqualTo(Credentials.longTermCredentials("myAccessKeyId", "mySecretAccessKey"));
        }
    }

    @Test
    void ec2MetadataCredentials() throws IOException {
        String json = """
                {
                    "endpointUrl": "https://kms.us-east-1.amazon.com",
                    "credentialsProvider": {
                        "type": "ec2Metadata",
                        "iamRole": "fooRole"
                    },
                    "region": "us-east-1"
                }
                """;
        Config config = readConfig(json);
        assertThat(config.credentialsProvider())
                .asInstanceOf(InstanceOfAssertFactories.type(Ec2MetadataCredentialsProviderConfig.class))
                .extracting(Ec2MetadataCredentialsProviderConfig::iamRole)
                .isEqualTo("fooRole");

        try (var provider = config.credentialsProvider().createCredentialsProvider()) {
            assertThat(provider).isNotNull();
        }
    }

    @Test
    void supportsLegacyCredentials() throws IOException {
        String json = """
                {
                    "endpointUrl": "http://kms.us-east-1.amazon.com",
                    "accessKey": {
                        "password": "myAccessKey"
                    },
                    "secretKey": {
                        "password": "mySecretKey"
                    },
                    "region": "us-east-1"
                }
                """;
        Config config = readConfig(json);
        assertThat(config.endpointUrl())
                .extracting(URI::toString)
                .isEqualTo("http://kms.us-east-1.amazon.com");
        try (var provider = config.credentialsProvider().createCredentialsProvider()) {
            assertThat(provider.getCredentials())
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isEqualTo(Credentials.longTermCredentials("myAccessKey", "mySecretKey"));
        }
    }

    private Config readConfig(String json) throws IOException {
        return MAPPER.reader().readValue(json, Config.class);
    }

}
