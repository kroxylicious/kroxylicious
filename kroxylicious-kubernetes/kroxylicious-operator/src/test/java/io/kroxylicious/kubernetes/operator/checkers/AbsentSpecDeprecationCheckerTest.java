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
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;
import org.threeten.extra.MutableClock;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.StatusFactory;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AbsentSpecDeprecationCheckerTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final ZoneId ZONE = ZoneId.of("UTC");
    private static final MutableClock TEST_CLOCK = MutableClock.of(NOW, ZONE);
    private static final String MESSAGE = "No spec, please add an empty one. Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release.";

    private StatusFactory<KafkaProxy> statusFactory;
    private AbsentSpecDeprecationChecker checker;
    private Logger logger;
    private LoggingEventBuilder eventBuilder;
    private ArrayList<Condition> existingConditions;

    @BeforeEach
    void setUp() {
        statusFactory = KafkaProxyReconciler.newStatusFactory(TEST_CLOCK);
        checker = new AbsentSpecDeprecationChecker();
        logger = mock(Logger.class);
        eventBuilder = mock(LoggingEventBuilder.class);
        existingConditions = spy(new ArrayList<>());
        when(eventBuilder.addKeyValue(anyString(), anyString())).thenReturn(eventBuilder);
        when(logger.atWarn()).thenReturn(eventBuilder);
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
    void absentSpecShouldLogWarning() {
        // Given
        var proxy = proxyWithoutSpec();
        var ctx = contextFor(proxy);

        // When
        checker.check(ctx);

        // Then
        verify(logger).atWarn();
        verify(eventBuilder).log(MESSAGE);
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
    void presentSpecShouldNotLog() {
        // Given
        var proxy = proxyWithSpec();
        var ctx = contextFor(proxy);

        // When
        checker.check(ctx);

        // Then
        verifyNoInteractions(logger);
    }

    @Test
    void resourceWithoutUidShouldResultInNothing() {
        // Given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("my-proxy")
                    .withNamespace("my-ns")
                .endMetadata()
                .build();
        var ctx = contextFor(proxy);
        // @formatter:on

        // When
        checker.check(ctx);

        // Then
        assertThat(ctx.conditions()).isEmpty();
        verifyNoInteractions(logger);
    }

    @Test
    void absentSpecOnSubsequentReconcileOfSameResourceShouldNotLogAgain() {
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
        // Warning logged only on the first reconcile
        verify(logger, times(1)).atWarn();
        // But the condition is still added on each reconcile
        assertThat(ctx1.conditions()).hasSize(1);
        assertThat(ctx2.conditions()).hasSize(1);
        // Existing condition passed through to preserve lastTransitionTime
        assertThat(ctx1.conditions()).hasSameElementsAs(ctx2.conditions());
    }

    @Test
    void absentSpecOnSubsequentReconcileOfSameResourceShouldLogAgainIfSuppressionWindowHasPassed() {
        // Given
        var proxy = proxyWithoutSpec();
        var ctx1 = contextFor(proxy);
        var ctx2 = contextFor(proxy);

        // When
        checker.check(ctx1);
        replaceExistingConditions(ctx1.conditions());
        TEST_CLOCK.add(Duration.ofHours(2));
        checker.check(ctx2);

        // Then
        // Warning logged on both reconciles
        verify(logger, times(2)).atWarn();
        // But the condition is still added on each reconcile
        assertThat(ctx1.conditions()).hasSize(1);
        assertThat(ctx2.conditions()).hasSize(1);
        // New condition created
        assertThat(ctx1.conditions()).doesNotContainAnyElementsOf(ctx2.conditions());
    }

    @Test
    void specRemovedAfterBeingPresentShouldLogWarningAgain() {
        // Given
        var uid = UUID.randomUUID().toString();
        var absentProxy = proxyBuilderWithUid(uid).build();
        var presentProxy = proxyBuilderWithUid(uid).withNewSpec().endSpec().build();

        var ctxAbsent1 = contextFor(absentProxy);
        var ctxPresent = contextFor(presentProxy);
        var ctxAbsent2 = contextFor(absentProxy);

        // When
        checker.check(ctxAbsent1); // spec absent = warn, condition added
        replaceExistingConditions(ctxAbsent1.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));
        checker.check(ctxPresent); // spec present = condition removed
        replaceExistingConditions(ctxPresent.conditions());
        TEST_CLOCK.add(Duration.ofMinutes(5));
        checker.check(ctxAbsent2); // spec absent again = warn again

        // Then
        verify(logger, times(2)).atWarn();
        assertThat(ctxAbsent1.conditions()).hasSize(1);
        assertThat(ctxPresent.conditions()).isEmpty();
        assertThat(ctxAbsent2.conditions()).hasSize(1);
        // New condition created
        assertThat(ctxAbsent1.conditions()).doesNotContainAnyElementsOf(ctxAbsent2.conditions());
    }

    private DeprecationCheckContext<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, StatusFactory<KafkaProxy>> contextFor(KafkaProxy proxy) {
        return new DeprecationCheckContext<>(proxy, logger, statusFactory, TEST_CLOCK, existingConditions);
    }

    private void replaceExistingConditions(List<Condition> newExistingConditions) {
        existingConditions.clear();
        existingConditions.addAll(newExistingConditions);
    }

    private static KafkaProxy proxyWithoutSpec() {
        return proxyBuilderWithUid(UUID.randomUUID().toString()).build();
    }

    private static KafkaProxy proxyWithSpec() {
        return proxyBuilderWithUid(UUID.randomUUID().toString()).withNewSpec().endSpec().build();
    }

    private static KafkaProxyBuilder proxyBuilderWithUid(String uid) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("my-proxy")
                    .withNamespace("my-ns")
                    .withUid(uid)
                .endMetadata();
        // @formatter:on
    }
}
