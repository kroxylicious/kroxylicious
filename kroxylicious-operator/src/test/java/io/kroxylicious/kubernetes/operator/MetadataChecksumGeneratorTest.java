/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Base64;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

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
        Base64.Decoder jdkDecoder = Base64.getDecoder();

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // Then
        Assertions.assertThat(checksum)
                .isBase64()
                .satisfies(string ->
                        Assertions.assertThatThrownBy(() -> Assertions.assertThat(string).asLong()));
        // A raw long is a valid base64 string, this assertion ensures we haven't just returned a long
    }

    @Test
    void shouldCalculateChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // Then
        Assertions.assertThat(checksum)
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
        Assertions.assertThat(checksum)
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
        Assertions.assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
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
        Assertions.assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }
}
