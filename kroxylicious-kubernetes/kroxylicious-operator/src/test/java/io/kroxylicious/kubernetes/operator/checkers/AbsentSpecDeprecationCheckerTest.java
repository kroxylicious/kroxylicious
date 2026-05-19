/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.StatusFactory;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;

import static org.assertj.core.api.Assertions.assertThat;

class AbsentSpecDeprecationCheckerTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final ZoneId ZONE = ZoneId.of("UTC");
    private static final MutableClock TEST_CLOCK = MutableClock.of(NOW, ZONE);
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
        var absentSpecContext = contextFor(proxy, List.of());

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
        var presentSpecContext = contextFor(proxy, List.of());

        // When
        checker.check(presentSpecContext);

        // Then
        assertThat(presentSpecContext.conditions()).isEmpty();
    }

    @Test
    void absentSpecOnSubsequentReconcileOfSameResourceShouldPreserveCondition() {
        // Given
        var proxy = proxyWithoutSpec();
        var absentSpecContextBefore = contextFor(proxy, List.of());

        checker.check(absentSpecContextBefore);
        var absentSpecContextAfter = contextFor(proxy, absentSpecContextBefore.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));

        // When
        checker.check(absentSpecContextAfter);

        // Then
        // The condition is added on each reconcile
        assertThat(absentSpecContextBefore.conditions()).hasSize(1);
        assertThat(absentSpecContextAfter.conditions()).hasSize(1);
        // Existing condition passed through to preserve lastTransitionTime
        assertThat(absentSpecContextBefore.conditions()).hasSameElementsAs(absentSpecContextAfter.conditions());
    }

    @Test
    void specRemovedAfterBeingPresentShouldResultInNewCondition() {
        // Given
        var absentProxy = proxyWithoutSpec();
        var presentProxy = proxyWithSpec();

        var absentSpecContextBefore = contextFor(absentProxy, List.of());
        checker.check(absentSpecContextBefore); // spec absent = condition added
        var presentSpecContext = contextFor(presentProxy, absentSpecContextBefore.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));

        checker.check(presentSpecContext); // spec present = condition removed
        var absentSpecContextAfter = contextFor(absentProxy, presentSpecContext.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));

        // When
        checker.check(absentSpecContextAfter); // spec absent again = condition added

        // Then
        assertThat(absentSpecContextBefore.conditions()).hasSize(1);
        assertThat(presentSpecContext.conditions()).isEmpty();
        assertThat(absentSpecContextAfter.conditions()).hasSize(1);
        // New condition created
        assertThat(absentSpecContextBefore.conditions()).doesNotContainAnyElementsOf(absentSpecContextAfter.conditions());
    }

    private DeprecationCheckContext<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, StatusFactory<KafkaProxy>> contextFor(KafkaProxy proxy,
                                                                                                                        List<Condition> existingConditions) {
        return new DeprecationCheckContext<>(proxy, statusFactory, existingConditions);
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
