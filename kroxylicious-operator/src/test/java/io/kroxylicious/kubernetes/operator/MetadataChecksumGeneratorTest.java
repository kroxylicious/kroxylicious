/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataChecksumGeneratorTest {
    private static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
            .withName("my-proxy")
            .withUid("my-proxy-uuid")
            .withGeneration(1L)
            .endMetadata()
            .build();
    // @formatter:on

    @Test
    void shouldBase64EncodeChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // Then
        assertThat(checksum)
                .isBase64()
                .satisfies(string -> Assertions.assertThatThrownBy(() -> assertThat(string).asLong()));
        // A raw long is a valid base64 string, this assertion ensures we haven't just returned a long
    }

    @Test
    void shouldCalculateChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isEqualTo("AAAAAHM+5SM");
    }

    @Test
    void shouldIncludeUidInChecksum() {
        // Given
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // When
        String checksum = MetadataChecksumGenerator
                .checksumFor(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build());

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationInChecksum() {
        // Given
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // When
        String checksum = MetadataChecksumGenerator
                .checksumFor(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(15L).endMetadata().build());

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    // some kubernetes objects like secret and configmap where the state cannot vary from the user's desired intent do not have a generation
    @Test
    void shouldIncludeResourceVersionInChecksumWhenGenerationIsNull() {
        // Given
        Secret secret = new SecretBuilder().withNewMetadata().withUid("uid").withResourceVersion("6432").endMetadata().build();
        String secretChecksum = MetadataChecksumGenerator.checksumFor(secret);

        // When
        String checksumWithDifferentResourceVersion = MetadataChecksumGenerator
                .checksumFor(new SecretBuilder(secret).editMetadata().withResourceVersion("6455").endMetadata().build());

        // Then
        assertThat(checksumWithDifferentResourceVersion)
                .isNotBlank()
                .isNotEqualTo(secretChecksum);
    }

    // if resource version changes and generation doesn't, for resources that have both properties, the checksum should not change
    @Test
    void shouldPreferGenerationInChecksumWhenGeneration() {
        // Given
        Secret secret = new SecretBuilder().withNewMetadata().withUid("uid").withResourceVersion("6432").withGeneration(1L).endMetadata().build();
        String secretChecksum = MetadataChecksumGenerator.checksumFor(secret);

        // When
        String checksumWithDifferentResourceVersion = MetadataChecksumGenerator
                .checksumFor(new SecretBuilder(secret).editMetadata().withResourceVersion("6478").withGeneration(1L).endMetadata().build());

        // Then
        assertThat(checksumWithDifferentResourceVersion)
                .isNotBlank()
                .isEqualTo(secretChecksum);
    }

    @Test
    void shouldIncludeMultipleReferentsInChecksum() {
        // Given
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(PROXY);
        KafkaProxy anotherProxy = new KafkaProxyBuilder()
                .withNewMetadataLike(PROXY.getMetadata())
                .withUid(UUID.randomUUID().toString())
                .withGeneration(15L)
                .endMetadata()
                .build();

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(PROXY, anotherProxy);

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);

        // Check it is deterministic
        assertThat(MetadataChecksumGenerator.checksumFor(PROXY, anotherProxy)).isEqualTo(checksum);
        assertThat(MetadataChecksumGenerator.checksumFor(PROXY)).isEqualTo(proxyChecksum);
    }
}
