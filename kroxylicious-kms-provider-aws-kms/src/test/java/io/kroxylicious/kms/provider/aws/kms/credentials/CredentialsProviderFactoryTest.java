/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.KmsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CredentialsProviderFactoryTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(YAMLFactory.builder().build());
    private static final CredentialsProviderFactory FACTORY = CredentialsProviderFactory.DEFAULT;

    @Test
    void legacyConfigLongTermCredentials() {
        // Given
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                accessKey:
                    password: accessKeyId
                secretKey:
                    password: secretAccessKey
                """);

        // When
        var cp = FACTORY.createCredentialsProvider(config);

        // Then
        assertThat(cp)
                .asInstanceOf(InstanceOfAssertFactories.type(LongTermCredentialsProvider.class))
                .satisfies(ltcp -> {
                    assertThat(ltcp.getCredentials())
                            .succeedsWithin(Duration.ofSeconds(1))
                            .satisfies(c -> {
                                assertThat(c.accessKeyId()).isEqualTo("accessKeyId");
                                assertThat(c.secretAccessKey()).isEqualTo("secretAccessKey");
                            });
                });
    }

    @Test
    void longTermCredentials() {
        // Given
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                longTermCredentials:
                    accessKeyId:
                        password: accessKeyId
                    secretAccessKey:
                        password: secretAccessKey
                """);

        // When
        var cp = FACTORY.createCredentialsProvider(config);

        // Then
        assertThat(cp)
                .asInstanceOf(InstanceOfAssertFactories.type(LongTermCredentialsProvider.class))
                .satisfies(ltcp -> {
                    assertThat(ltcp.getCredentials())
                            .succeedsWithin(Duration.ofSeconds(1))
                            .satisfies(c -> {
                                assertThat(c.accessKeyId()).isEqualTo("accessKeyId");
                                assertThat(c.secretAccessKey()).isEqualTo("secretAccessKey");
                            });
                });
        cp.close();
    }

    @Test
    void ec2MetadataCredentials() {
        // Given
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                ec2MetadataCredentials:
                    iamRole: foo
                """);

        // When
        var cp = FACTORY.createCredentialsProvider(config);

        // Then
        assertThat(cp)
                .isInstanceOf(Ec2MetadataCredentialsProvider.class)
                .isNotNull();
        cp.close();
    }

    public static Stream<Arguments> invalidConfig() {
        return Stream.of(
                Arguments.argumentSet("no credential provider", """
                            endpointUrl: https://unused.invalid
                            region: unused
                        """),
                Arguments.argumentSet("ec2Metadata and longTerm disallowed", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            ec2MetadataCredentials:
                                iamRole: foo
                            longTermCredentials:
                                accessKeyId:
                                    password: accessKeyId
                                secretAccessKey:
                                    password: secretAccessKey
                        """),
                Arguments.argumentSet("longTerm with legacy 1", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            accessKey:
                                password: accessKeyId
                            longTermCredentials:
                                accessKeyId:
                                    password: accessKeyId
                                secretAccessKey:
                                    password: secretAccessKey
                        """),
                Arguments.argumentSet("longTerm with legacy 2", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            secretKey:
                                password: secretAccessKey
                            longTermCredentials:
                                accessKeyId:
                                    password: accessKeyId
                                secretAccessKey:
                                    password: secretAccessKey
                        """),
                Arguments.argumentSet("ec2Metadata with legacy 1", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            accessKey:
                                password: accessKeyId
                            ec2MetadataCredentials:
                                iamRole: foo
                        """),
                Arguments.argumentSet("ec2Metadata with legacy 2", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            secretKey:
                                password: secretAccessKey
                            ec2MetadataCredentials:
                                iamRole: foo
                        """),
                Arguments.argumentSet("partially defined legacy 1", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            accessKey:
                                password: accessKeyId
                        """),
                Arguments.argumentSet("partially defined legacy 2", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            secretKey:
                                password: secretAccessKey
                        """)

        );
    }

    @ParameterizedTest
    @MethodSource("invalidConfig")
    @SuppressWarnings("resource")
    void rejectsInvalidConfig(String yaml) {
        // Given
        var config = buildConfig(yaml);

        // When/Then
        assertThatThrownBy(() -> FACTORY.createCredentialsProvider(config))
                .isInstanceOf(KmsException.class);

    }

    private Config buildConfig(String content) {
        try {
            return OBJECT_MAPPER.readValue(content, Config.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
