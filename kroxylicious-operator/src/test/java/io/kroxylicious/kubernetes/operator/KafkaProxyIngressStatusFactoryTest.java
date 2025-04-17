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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;

class KafkaProxyIngressStatusFactoryTest {
    private static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    public static final String CHECKSUM = "AAAAAPLX/Dk";
    private KafkaProxyIngressStatusFactory kafkaProxyIngressStatusFactory;

    // @formatter:off
    public static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName("foo")
                .withGeneration(42L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .withNewSpec()
                .withNewProxyRef()
                    .withName("my-proxy")
                .endProxyRef()
            .endSpec()
            .build();
    // @formatter:on

    @BeforeEach
    void setUp() {
        kafkaProxyIngressStatusFactory = new KafkaProxyIngressStatusFactory(TEST_CLOCK);
    }

    @Test
    void shouldAddReferentsChecksumAnnotation() {
        // Given

        // When
        KafkaProxyIngress kafkaProxyIngress = kafkaProxyIngressStatusFactory.newTrueConditionStatusPatch(INGRESS, Condition.Type.ResolvedRefs, CHECKSUM);

        // Then
        assertThat(kafkaProxyIngress)
                .hasAnnotationSatisfying("kroxylicious.io/referent-checksum", checksum -> Assertions.assertThat(checksum).isEqualTo(CHECKSUM));
    }
}