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

    // --- New-style credentials node ---

    @Test
    void longTermCredentialsNewStyle() {
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                credentials:
                    longTerm:
                        accessKeyId:
                            password: accessKeyId
                        secretAccessKey:
                            password: secretAccessKey
                """);

        var cp = FACTORY.createCredentialsProvider(config);

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
    void ec2MetadataCredentialsNewStyle() {
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                credentials:
                    ec2Metadata:
                        iamRole: foo
                """);

        var cp = FACTORY.createCredentialsProvider(config);

        assertThat(cp)
                .isInstanceOf(Ec2MetadataCredentialsProvider.class)
                .isNotNull();
        cp.close();
    }

    @Test
    void webIdentityCredentials() {
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: us-east-1
                credentials:
                    webIdentity:
                        roleArn: arn:aws:iam::123456789012:role/r
                        webIdentityTokenFile: /var/run/secrets/eks.amazonaws.com/serviceaccount/token
                """);

        var cp = FACTORY.createCredentialsProvider(config);

        assertThat(cp)
                .isInstanceOf(WebIdentityCredentialsProvider.class)
                .isNotNull();
        cp.close();
    }

    @Test
    void podIdentityCredentials() {
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: us-east-1
                credentials:
                    podIdentity:
                        credentialsFullUri: http://169.254.170.23/v1/credentials
                        authorizationTokenFile: /var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token
                """);

        var cp = FACTORY.createCredentialsProvider(config);

        assertThat(cp)
                .isInstanceOf(PodIdentityCredentialsProvider.class)
                .isNotNull();
        cp.close();
    }

    // --- Backward-compat: deprecated flat fields ---

    @Test
    void longTermCredentialsBackwardCompat() {
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                longTermCredentials:
                    accessKeyId:
                        password: accessKeyId
                    secretAccessKey:
                        password: secretAccessKey
                """);

        var cp = FACTORY.createCredentialsProvider(config);

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
    void ec2MetadataCredentialsBackwardCompat() {
        var config = buildConfig("""
                endpointUrl: https://unused.invalid
                region: unused
                ec2MetadataCredentials:
                    iamRole: foo
                """);

        var cp = FACTORY.createCredentialsProvider(config);

        assertThat(cp)
                .isInstanceOf(Ec2MetadataCredentialsProvider.class)
                .isNotNull();
        cp.close();
    }

    // --- Invalid configs ---

    public static Stream<Arguments> invalidConfig() {
        return Stream.of(
                Arguments.argumentSet("no credential provider", """
                            endpointUrl: https://unused.invalid
                            region: unused
                        """),
                Arguments.argumentSet("multiple providers in credentials node", """
                            endpointUrl: https://unused.invalid
                            region: us-east-1
                            credentials:
                                ec2Metadata:
                                    iamRole: foo
                                longTerm:
                                    accessKeyId:
                                        password: accessKeyId
                                    secretAccessKey:
                                        password: secretAccessKey
                        """),
                Arguments.argumentSet("webIdentity and podIdentity conflict", """
                            endpointUrl: https://unused.invalid
                            region: us-east-1
                            credentials:
                                webIdentity:
                                    roleArn: arn:aws:iam::123456789012:role/r
                                    webIdentityTokenFile: /tmp/token
                                podIdentity:
                                    credentialsFullUri: http://169.254.170.23/v1/credentials
                                    authorizationTokenFile: /tmp/agent-token
                        """),
                Arguments.argumentSet("deprecated longTerm and ec2Metadata conflict", """
                            endpointUrl: https://unused.invalid
                            region: unused
                            longTermCredentials:
                                accessKeyId:
                                    password: accessKeyId
                                secretAccessKey:
                                    password: secretAccessKey
                            ec2MetadataCredentials:
                                iamRole: foo
                        """),
                Arguments.argumentSet("deprecated flat field with credentials node conflict", """
                            endpointUrl: https://unused.invalid
                            region: us-east-1
                            longTermCredentials:
                                accessKeyId:
                                    password: accessKeyId
                                secretAccessKey:
                                    password: secretAccessKey
                            credentials:
                                webIdentity:
                                    roleArn: arn:aws:iam::123456789012:role/r
                                    webIdentityTokenFile: /tmp/token
                        """));
    }

    @ParameterizedTest
    @MethodSource("invalidConfig")
    @SuppressWarnings("resource")
    void rejectsInvalidConfig(String yaml) {
        // KmsException may be thrown directly (factory-time) or wrapped in
        // ValueInstantiationException (deserialization-time, e.g. when the
        // compact constructor rejects conflicting deprecated + new-style fields).
        assertThatThrownBy(() -> {
            var config = buildConfig(yaml);
            FACTORY.createCredentialsProvider(config);
        }).satisfiesAnyOf(
                t -> assertThat(t).isInstanceOf(KmsException.class),
                t -> assertThat(t).rootCause().isInstanceOf(KmsException.class));
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
