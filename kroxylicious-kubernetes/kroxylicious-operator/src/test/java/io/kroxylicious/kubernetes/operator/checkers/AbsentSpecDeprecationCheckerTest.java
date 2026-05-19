/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.operator.StatusFactory;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;

import static org.assertj.core.api.Assertions.assertThat;

class AbsentSpecDeprecationCheckerTest {

    private static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    private static final String MESSAGE = "No spec, please add an empty one. Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release.";

    private StatusFactory<KafkaProxy> statusFactory;
    private AbsentSpecDeprecationChecker checker;

    @BeforeEach
    void setUp() {
        statusFactory = KafkaProxyReconciler.newStatusFactory(TEST_CLOCK);
        checker = new AbsentSpecDeprecationChecker();
    }

    @Test
    void absentSpecShouldAddDeprecationWarningCondition() {
        // Given
        var proxy = proxyWithoutSpec();
        var absentSpecContext = new DeprecationCheckContext<>(proxy, statusFactory);

        // When
        checker.check(absentSpecContext);

        // Then
        assertThat(absentSpecContext.conditions())
                .hasSize(1)
                .singleElement()
                .satisfies(condition -> {
                    assertThat(condition.getType()).isEqualTo(Condition.Type.DeprecationWarning);
                    assertThat(condition.getStatus()).isEqualTo(Condition.Status.TRUE);
                    assertThat(condition.getReason()).isEqualTo(Condition.Type.DeprecationWarning.name());
                    assertThat(condition.getMessage()).isEqualTo(MESSAGE);
                });
    }

    @Test
    void presentSpecShouldNotAddCondition() {
        // Given
        var proxy = proxyWithSpec();
        var presentSpecContext = new DeprecationCheckContext<>(proxy, statusFactory);

        // When
        checker.check(presentSpecContext);

        // Then
        assertThat(presentSpecContext.conditions()).isEmpty();
    }

    private static KafkaProxy proxyWithoutSpec() {
        return proxyBuilder().build();
    }

    private static KafkaProxy proxyWithSpec() {
        return proxyBuilder().withNewSpec().endSpec().build();
    }

    private static KafkaProxyBuilder proxyBuilder() {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("my-proxy")
                    .withNamespace("my-ns")
                .endMetadata();
        // @formatter:on
    }
}
