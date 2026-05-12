/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyStatusFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AbsentSpecDeprecationCheckerTest {

    private static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    private static final String MESSAGE = "No spec, please add an empty one. Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release.";

    private KafkaProxyStatusFactory statusFactory;
    private AbsentSpecDeprecationChecker checker;
    private Logger logger;
    private LoggingEventBuilder eventBuilder;

    @BeforeEach
    void setUp() {
        statusFactory = new KafkaProxyStatusFactory(TEST_CLOCK);
        checker = new AbsentSpecDeprecationChecker();
        logger = mock(Logger.class);
        eventBuilder = mock(LoggingEventBuilder.class);
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
        checker.check(ctx2);

        // Then
        // Warning logged only on the first reconcile
        verify(logger, times(1)).atWarn();
        // But the condition is still added on each reconcile
        assertThat(ctx1.conditions()).hasSize(1);
        assertThat(ctx2.conditions()).hasSize(1);
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
        checker.check(ctxAbsent1); // spec absent = warn, cache populated
        checker.check(ctxPresent); // spec present = cache invalidated
        checker.check(ctxAbsent2); // spec absent again = warn again

        // Then
        verify(logger, times(2)).atWarn();
        assertThat(ctxAbsent1.conditions()).hasSize(1);
        assertThat(ctxPresent.conditions()).isEmpty();
        assertThat(ctxAbsent2.conditions()).hasSize(1);
    }

    private DeprecationCheckContext<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, KafkaProxyStatusFactory> contextFor(KafkaProxy proxy) {
        return new DeprecationCheckContext<>(proxy, logger, statusFactory);
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
