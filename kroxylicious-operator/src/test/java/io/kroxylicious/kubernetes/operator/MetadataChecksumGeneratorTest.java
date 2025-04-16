/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

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
            .withNewSpec()
            .endSpec()
            .build();

    @Test
    void shouldCalculateChecksum() {
        // Given

        // When
        String checksum = MetadataChecksumGenerator.checksumFor(PROXY);

        // Then
        Assertions.assertThat(checksum)
                .isNotBlank()
                .isEqualTo("1933501731");
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
}
