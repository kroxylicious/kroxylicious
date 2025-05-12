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
import org.junit.jupiter.api.Test;

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
    // @formatter:on

    @Test
    void shouldBase64EncodeChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // Then
        assertThat(checksum)
                .isBase64()
                .satisfies(string -> Assertions.assertThatThrownBy(() -> assertThat(string).asLong()));
        // A raw long is a valid base64 string, this assertion ensures we haven't just returned a long
    }

    @Test
    void emptyInputShouldResultInUnspecifiedChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(List.of());

        // Then
        assertThat(checksum).isEqualTo(MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
    }

    @Test
    void shouldCalculateChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isEqualTo("AAAAAHM+5SM");
    }

    @Test
    void shouldGenerationSameChecksumForTheSameInput() {
        // Given
        String checksumA = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        String checksumB = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // Then
        assertThat(checksumA).isEqualTo(checksumB);
    }

    @Test
    void shouldIncludeUidInChecksum() {
        // Given
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(List.of(
                new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build()));

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationInChecksum() {
        // Given
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(List.of(
                new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(15L).endMetadata().build()));

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldAppendGenerationToChecksum() {
        // Given
        Crc32ChecksumGenerator generator = new Crc32ChecksumGenerator();
        Crc32ChecksumGenerator generator2 = new Crc32ChecksumGenerator();
        generator.appendMetadata(PROXY);
        String proxyChecksum = generator.encode();
        KafkaProxy updatedProxy = new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(15L).endMetadata().build();

        // When
        generator2.appendMetadata(updatedProxy);

        // Then
        assertThat(generator2.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldGenerateDifferentChecksumsForRepeatedCallsToAppendLong() {
        // Given
        Crc32ChecksumGenerator generator = new Crc32ChecksumGenerator();
        generator.appendLong(1L);
        long checksumGeneration1 = generator.getValue();

        // When
        generator.appendLong(2L);

        // Then
        var checksumGeneration2 = generator.getValue();
        assertThat(checksumGeneration2)
                .isNotZero()
                .isNotEqualTo(checksumGeneration1);
    }
    
    // some kubernetes objects like secret and configmap where the state cannot vary from the user's desired intent do not have a generation
    @Test
    void shouldIncludeResourceVersionInChecksumWhenGenerationIsNull() {
        // Given
        Secret secret = new SecretBuilder().withNewMetadata().withUid("uid").withResourceVersion("6432").endMetadata().build();
        String secretChecksum = MetadataChecksumGenerator.checksumFor((List.of(secret)));

        // When
        String checksumWithDifferentResourceVersion = MetadataChecksumGenerator
                .checksumFor(List.of(new SecretBuilder(secret).editMetadata().withResourceVersion("6455").endMetadata().build()));

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
        String secretChecksum = MetadataChecksumGenerator.checksumFor(List.of(secret));

        // When
        String checksumWithDifferentResourceVersion = MetadataChecksumGenerator.checksumFor(List.of(
                new SecretBuilder(secret).editMetadata().withResourceVersion("6478").withGeneration(1L).endMetadata().build()));

        // Then
        assertThat(checksumWithDifferentResourceVersion)
                .isNotBlank()
                .isEqualTo(secretChecksum);
    }

    @Test
    void shouldIncludeMultipleReferentsInChecksum() {
        // Given
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));
        KafkaProxy anotherProxy = new KafkaProxyBuilder()
                .withNewMetadataLike(PROXY.getMetadata())
                .withUid(UUID.randomUUID().toString())
                .withGeneration(15L)
                .endMetadata()
                .build();

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY, anotherProxy));

        // Then
        assertThat(checksum)
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);

        // Check it is deterministic
        assertThat(MetadataChecksumGenerator.checksumFor(List.of(PROXY, anotherProxy))).isEqualTo(checksum);
        assertThat(MetadataChecksumGenerator.checksumFor(List.of(PROXY))).isEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeUidFromHasMetadata() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator.appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build());

        // Then
        assertThat(metadataChecksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeUidFromObjectMeta() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withUid("updated-uid").endMetadata().build().getMetadata());

        // Then
        assertThat(metadataChecksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationFromHasMetadata() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator.appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(3456789L).endMetadata().build());

        // Then
        assertThat(metadataChecksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeGenerationFromObjectMeta() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata()).withGeneration(3456789L).endMetadata().build().getMetadata());

        // Then
        assertThat(metadataChecksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIncludeReferentAnnotation() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata())
                        .withAnnotations(Map.of(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION, "checksumB")).endMetadata().build().getMetadata());

        // Then
        assertThat(metadataChecksumGenerator.encode())
                .isNotBlank()
                .isNotEqualTo(proxyChecksum);
    }

    @Test
    void shouldIgnoreOtherAnnotation() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata())
                        .withAnnotations(Map.of("annotation1", "value1")).endMetadata().build().getMetadata());

        // Then
        assertThat(metadataChecksumGenerator.encode())
                .isNotBlank()
                .isEqualTo(proxyChecksum);
    }

    @Test
    void shouldIgnoreNullAnnotation() {
        // Given
        MetadataChecksumGenerator metadataChecksumGenerator = new Crc32ChecksumGenerator();
        String proxyChecksum = MetadataChecksumGenerator.checksumFor(List.of(PROXY));

        // When
        metadataChecksumGenerator
                .appendMetadata(new KafkaProxyBuilder().withNewMetadataLike(PROXY.getMetadata())
                        .withAnnotations(null).endMetadata().build().getMetadata());

        // Then
        assertThat(metadataChecksumGenerator.encode())
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
}
