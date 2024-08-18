/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import io.kroxylicious.kms.provider.aws.kms.credentials.FixedCredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ConfigParseTest {
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

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
                    .isEqualTo(FixedCredentialsProvider.fixedCredentials("myAccessKeyId", "mySecretAccessKey"));
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
                    .isEqualTo(FixedCredentialsProvider.fixedCredentials("myAccessKey", "mySecretKey"));
        }
    }

    static Stream<Arguments> disallowsLegacyCredentialsWithCredentialsProvider() {
        return Stream.of(arguments("accessKey",
                """
                        {
                            "endpointUrl": "http://kms.us-east-1.amazon.com",
                            "credentialsProvider": {
                                "type": "fixed",
                                "accessKeyId": {
                                    "password": "myAccessKeyId"
                                },
                                "secretAccessKey": {
                                    "password": "mySecretAccessKey"
                                }
                            },
                            "accessKey": {
                                "password": "myAccessKey"
                            },
                            "region": "us-east-1"
                        }
                        """),
                arguments("secretKey",
                        """
                                {
                                    "endpointUrl": "http://kms.us-east-1.amazon.com",
                                    "credentialsProvider": {
                                        "type": "fixed",
                                        "accessKeyId": {
                                            "password": "myAccessKeyId"
                                        },
                                        "secretAccessKey": {
                                            "password": "mySecretAccessKey"
                                        }
                                    },
                                    "secretKey": {
                                        "password": "mySecretKey"
                                    },
                                    "region": "us-east-1"
                                }
                                """));
    }

    @ParameterizedTest
    @MethodSource
    void disallowsLegacyCredentialsWithCredentialsProvider(String field, String json) {
        assertThatThrownBy(() -> readConfig(json))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot provide " + field + " when using credentialsProvider");
    }

    private Config readConfig(String json) throws IOException {
        return MAPPER.reader().readValue(json, Config.class);
    }

}
