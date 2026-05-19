/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
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
    private ArrayList<Condition> existingConditions;

    @BeforeEach
    void setUp() {
        statusFactory = KafkaProxyReconciler.newStatusFactory(TEST_CLOCK);
        checker = new AbsentSpecDeprecationChecker();
        existingConditions = new ArrayList<>();
    }

    @Test
    void absentSpecShouldAddDeprecationWarningCondition() {
        // Given
        var proxy = proxyWithoutSpec();
        var ctx = contextFor(proxy);

        // When
        checker.check(ctx);

        // Then
        assertThat(ctx.conditions())
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
        var ctx = contextFor(proxy);

        // When
        checker.check(ctx);

        // Then
        assertThat(ctx.conditions()).isEmpty();
    }

    @Test
    void absentSpecOnSubsequentReconcileOfSameResourceShouldPreserveCondition() {
        // Given
        var proxy = proxyWithoutSpec();
        var ctx1 = contextFor(proxy);
        var ctx2 = contextFor(proxy);

        // When
        checker.check(ctx1);
        replaceExistingConditions(ctx1.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));
        checker.check(ctx2);

        // Then
        // The condition is added on each reconcile
        assertThat(ctx1.conditions()).hasSize(1);
        assertThat(ctx2.conditions()).hasSize(1);
        // Existing condition passed through to preserve lastTransitionTime
        assertThat(ctx1.conditions()).hasSameElementsAs(ctx2.conditions());
    }

    @Test
    void specRemovedAfterBeingPresentShouldResultInNewCondition() {
        // Given
        var absentProxy = proxyWithoutSpec();
        var presentProxy = proxyWithSpec();

        var ctxAbsent1 = contextFor(absentProxy);
        var ctxPresent = contextFor(presentProxy);
        var ctxAbsent2 = contextFor(absentProxy);

        // When
        checker.check(ctxAbsent1); // spec absent = condition added
        replaceExistingConditions(ctxAbsent1.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));
        checker.check(ctxPresent); // spec present = condition removed
        replaceExistingConditions(ctxPresent.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));
        checker.check(ctxAbsent2); // spec absent again = condition added

        // Then
        assertThat(ctxAbsent1.conditions()).hasSize(1);
        assertThat(ctxPresent.conditions()).isEmpty();
        assertThat(ctxAbsent2.conditions()).hasSize(1);
        // New condition created
        assertThat(ctxAbsent1.conditions()).doesNotContainAnyElementsOf(ctxAbsent2.conditions());
    }

    private DeprecationCheckContext<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, StatusFactory<KafkaProxy>> contextFor(KafkaProxy proxy) {
        return new DeprecationCheckContext<>(proxy, statusFactory, existingConditions);
    }

    private void replaceExistingConditions(List<Condition> newExistingConditions) {
        existingConditions.clear();
        existingConditions.addAll(newExistingConditions);
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
