/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checksum;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class Crc32ChecksumGeneratorTest {
    private static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
            .withName("my-proxy")
            .withUid("my-proxy-uuid")
            .withGeneration(1L)
            .endMetadata()
            .build();
    private Crc32ChecksumGenerator checksumGenerator;
    // @formatter:on

    @BeforeEach
    void setUp() {
        checksumGenerator = new Crc32ChecksumGenerator();
    }

    @Test
    void shouldBase64EncodeChecksum() {
        // Given
        checksumGenerator.appendMetadata(PROXY);

        // When
        String encoded = checksumGenerator.encode();

        // Then
        assertThat(encoded)
                .isBase64()
                .satisfies(value -> Assertions.assertThatThrownBy(() -> assertThat(value).asLong()));
        // A raw long is a valid base64 string, this assertion ensures we haven't just returned a long
    }

    @Test
    void emptyInputShouldResultInUnspecifiedChecksum() {
        // Given

        // When
        String checksum = checksumGenerator.encode();

        // Then
        assertThat(checksum).isEqualTo(MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
    }

    @Test
    void shouldCalculateChecksum() {
        // Given
        checksumGenerator.appendMetadata(PROXY);

        // When
        String checksum = checksumGenerator.encode();

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isEqualTo("AAAAAHM+5SM");
    }

    @Test
    void shouldGenerationSameChecksumForTheSameInput() {
        // Given
        String checksumA = generateProxyChecksum();

        checksumGenerator.appendMetadata(PROXY);

        // When
        String checksumB = checksumGenerator.encode();

        // Then
        assertThat(checksumA).isEqualTo(checksumB);
    }

    @Test
    void shouldIncludeUidInChecksum() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator.appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationInChecksum() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator.appendMetadata(
                new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(15L).endMetadata().build());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldAppendGenerationToChecksum() {
        // Given
        String proxyChecksum = generateProxyChecksum();
        KafkaProxy updatedProxy = new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(15L).endMetadata().build();

        // When
        checksumGenerator.appendMetadata(updatedProxy);

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldGenerateDifferentChecksumsForRepeatedCallsToAppendLong() {
        // Given
        checksumGenerator.appendLong(1L);
        long checksumGeneration1 = checksumGenerator.getValue();

        // When
        checksumGenerator.appendLong(2L);

        // Then
        assertThat(checksumGenerator.getValue())
                .isNotZero()
                .isNotEqualTo(checksumGeneration1);
    }

    // some kubernetes objects like secret and configmap where the state cannot vary from the user's desired intent do not have a generation
    @Test
    void shouldIncludeResourceVersionInChecksumWhenGenerationIsNull() {
        // Given
        Secret secret = new SecretBuilder().withNewMetadata().withUid("uid").withResourceVersion("6432").endMetadata().build();
        Crc32ChecksumGenerator secretChecksumGenerator = new Crc32ChecksumGenerator();
        secretChecksumGenerator.appendMetadata(secret);
        String secretChecksum = secretChecksumGenerator.encode();

        // When
        checksumGenerator.appendMetadata(new SecretBuilder(secret).editMetadata().withResourceVersion("6455").endMetadata().build());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(secretChecksum);
    }

    // if resource version changes and generation doesn't, for resources that have both properties, the checksum should not change
    @Test
    void shouldPreferGenerationInChecksumWhenGeneration() {
        // Given
        Secret secret = new SecretBuilder().withNewMetadata().withUid("uid").withResourceVersion("6432").withGeneration(1L).endMetadata().build();
        Crc32ChecksumGenerator secretChecksumGenerator = new Crc32ChecksumGenerator();
        secretChecksumGenerator.appendMetadata(secret);
        String secretChecksum = secretChecksumGenerator.encode();

        // When
        checksumGenerator.appendMetadata(new SecretBuilder(secret).editMetadata().withResourceVersion("6478").withGeneration(1L).endMetadata().build());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isEqualTo(secretChecksum);
    }

    @Test
    void shouldIncludeMultipleReferentsInChecksum() {
        // Given
        String proxyChecksum = generateProxyChecksum();
        KafkaProxy anotherProxy = new KafkaProxyBuilder()
                .withNewMetadataLike(PROXY.getMetadata())
                .withUid(UUID.randomUUID().toString())
                .withGeneration(15L)
                .endMetadata()
                .build();

        checksumGenerator.appendMetadata(PROXY);

        // When
        checksumGenerator.appendMetadata(anotherProxy);

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);

        // Check it is deterministic

        List<? extends HasMetadata> metadataSources1 = List.of(PROXY, anotherProxy);
        Crc32ChecksumGenerator checksumGenerator1 = new Crc32ChecksumGenerator();
        checksumGenerator1.appendMetadata(PROXY);
        checksumGenerator1.appendMetadata(anotherProxy);
        assertThat(checksumGenerator1.encode()).isEqualTo(checksumGenerator.encode());

        assertThat(generateProxyChecksum()).isEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeUidFromHasMetadata() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator.appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeUidFromObjectMeta() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build().getMetadata());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationFromHasMetadata() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator.appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(3456789L).endMetadata().build());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationFromObjectMeta() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(3456789L).endMetadata().build().getMetadata());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeReferentAnnotation() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata())
                        .withAnnotations(Map.of(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION, "checksumB")).endMetadata().build().getMetadata());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIgnoreOtherAnnotation() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata())
                        .withAnnotations(Map.of("annotation1", "value1")).endMetadata().build().getMetadata());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isEqualTo(proxyChecksum);
    }

    @Test
    void shouldIgnoreNullAnnotation() {
        // Given
        String proxyChecksum = generateProxyChecksum();

        // When
        checksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata())
                        .withAnnotations(null).endMetadata().build().getMetadata());

        // Then
        assertThat(checksumGenerator.encode())
                .isNotBlank()
                .isEqualTo(proxyChecksum);
    }

    @Test
    void shouldNotEncodeIfValueIsZero() {
        // Given

        // When
        String encoded = new Crc32ChecksumGenerator().encode();

        // Then
        assertThat(encoded).isBlank();
    }

    private static String generateProxyChecksum() {
        var generator = new Crc32ChecksumGenerator();
        generator.appendMetadata(PROXY);
        return generator.encode();
    }
}
