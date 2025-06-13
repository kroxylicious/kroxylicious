/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.assertj.AssertFactory;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProxyStatusAssert;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;

class KafkaProxyStatusFactoryTest {
    private static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    public static final String CHECKSUM = "AAAAAPLX/Dk";
    private KafkaProxyStatusFactory kafkaProxyStatusFactory;

    // @formatter:off
    public static final KafkaProxy KAFKA_PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
              .withName("kafkaProxy")
                .withGeneration(42L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    // @formatter:on

    @BeforeEach
    void setUp() {
        kafkaProxyStatusFactory = new KafkaProxyStatusFactory(TEST_CLOCK);
    }

    @Test
    void shouldNotAddReferentsChecksumAnnotation() {
        // Given

        // When
        KafkaProxy patchedProxy = kafkaProxyStatusFactory.newTrueConditionStatusPatch(KAFKA_PROXY, Condition.Type.ResolvedRefs, CHECKSUM);

        // Then
        assertThat(patchedProxy).doesNotHaveAnnotation(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY);
    }

    @Test
    void shouldNotAddBlankReferentsChecksumAnnotation() {
        // Given

        // When
        KafkaProxy patchedProxy = kafkaProxyStatusFactory.newTrueConditionStatusPatch(KAFKA_PROXY, Condition.Type.ResolvedRefs,
                MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);

        // Then
        assertThat(patchedProxy).doesNotHaveAnnotation(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY);
    }

    @Test
    void shouldReferentsChecksumAnnotationToFalseCondition() {
        // Given

        // When
        KafkaProxy patchedProxy = kafkaProxyStatusFactory.newFalseConditionStatusPatch(KAFKA_PROXY, Condition.Type.ResolvedRefs, "reason", "message");

        // Then
        assertThat(patchedProxy).doesNotHaveAnnotation(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY);
    }

    @Test
    void shouldReferentsChecksumAnnotationToUnknownCondition() {
        // Given

        // When
        KafkaProxy patchedProxy = kafkaProxyStatusFactory.newUnknownConditionStatusPatch(KAFKA_PROXY, Condition.Type.ResolvedRefs,
                new RuntimeException("whoopsie it broke"));

        // Then
        assertThat(patchedProxy).doesNotHaveAnnotation(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY);
    }

    @Test
    void shouldIncludeReplicaCountInStatus() {
        // Given

        // When
        KafkaProxy patchedProxy = kafkaProxyStatusFactory.newTrueConditionStatusPatch(KAFKA_PROXY, Condition.Type.ResolvedRefs, 5);

        // Then
        assertThat(patchedProxy)
                .isNotNull()
                .extracting(KafkaProxy::getStatus, AssertFactory.status())
                .replicas(5);
    }
}
