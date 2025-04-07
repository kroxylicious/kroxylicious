/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

import org.assertj.core.api.AbstractIntegerAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;

import static io.kroxylicious.kubernetes.operator.ResourceState.FRESHEST_CONDITION;
import static io.kroxylicious.kubernetes.operator.ResourceStateTest.CompareToResult.EQUAL;
import static io.kroxylicious.kubernetes.operator.ResourceStateTest.CompareToResult.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

class ResourceStateTest {

    private static final Clock TEST_TIME = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

    @Test
    void shouldReturnConditionWithLargerGeneration() {
        var c12 = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .build();

        var c13 = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .build();

        var c13True = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .build();

        assertThat(ResourceState.newConditions(List.of(c12), ResourceState.fromList(List.of(c13)))).isEqualTo(List.of(c13));
        assertThat(ResourceState.newConditions(List.of(c13), ResourceState.fromList(List.of(c12)))).isEqualTo(List.of(c13));

        assertThat(ResourceState.newConditions(List.of(c12), ResourceState.fromList(List.of(c13True)))).isEqualTo(List.of(c13True));
        assertThat(ResourceState.newConditions(List.of(c13True), ResourceState.fromList(List.of(c12)))).isEqualTo(List.of(c13True));
    }

    public static Stream<Arguments> testComparator() {
        Instant now = Instant.now();
        Condition template = new ConditionBuilder().withStatus(Condition.Status.FALSE)
                .withObservedGeneration(1L)
                .withLastTransitionTime(now).build();
        Condition nullObservedGeneration = template.edit().withObservedGeneration(null).build();
        Condition observedGenerationOne = template.edit().withObservedGeneration(1L).build();
        Condition observedGenerationTwo = template.edit().withObservedGeneration(2L).build();
        Condition nullStatus = template.edit().withStatus(null).build();
        Condition falseStatus = template.edit().withStatus(Condition.Status.FALSE).build();
        Condition trueStatus = template.edit().withStatus(Condition.Status.TRUE).build();
        Condition unknownStatus = template.edit().withStatus(Condition.Status.UNKNOWN).build();
        Condition nullLastTransitionTime = template.edit().withLastTransitionTime(null).build();
        Condition lastTransitionTimeEpoch = template.edit().withLastTransitionTime(Instant.EPOCH).build();
        Condition lastTransitionTimeEpochPlusOneMin = template.edit().withLastTransitionTime(Instant.EPOCH.plus(1, ChronoUnit.MINUTES)).build();
        return Stream.of(Arguments.argumentSet("higher generation is greater than lower generation", observedGenerationOne, observedGenerationTwo, LESS_THAN),
                Arguments.argumentSet("non-null generation is higher than null generation", nullObservedGeneration, observedGenerationTwo, LESS_THAN),
                Arguments.argumentSet("same generation, fresher transition time is higher", lastTransitionTimeEpoch, lastTransitionTimeEpochPlusOneMin, LESS_THAN),
                Arguments.argumentSet("same generation, any transition time higher than null transition time", nullLastTransitionTime, lastTransitionTimeEpoch,
                        LESS_THAN),
                Arguments.argumentSet("same generation, same transition time, any status is higher than null status", nullStatus, unknownStatus, LESS_THAN),
                Arguments.argumentSet("same generation, same transition time, unknown status is higher than false", falseStatus, unknownStatus, LESS_THAN),
                Arguments.argumentSet("same generation, same transition time, false status is higher than true", trueStatus, falseStatus, LESS_THAN),
                Arguments.argumentSet("same generation, same transition time, unknown status is higher than true", trueStatus, unknownStatus, LESS_THAN),
                Arguments.argumentSet("same generation, same transition time, same status is equal", observedGenerationOne, observedGenerationOne, EQUAL));
    }

    enum CompareToResult {
        LESS_THAN,
        EQUAL
    }

    @MethodSource
    @ParameterizedTest
    void testComparator(Condition a, Condition b, CompareToResult expectedCompareTo) {
        AbstractIntegerAssert<?> assertThat = assertThat(FRESHEST_CONDITION.compare(a, b));
        AbstractIntegerAssert<?> assertThatInverse = assertThat(FRESHEST_CONDITION.compare(b, a));
        switch (expectedCompareTo) {
            case LESS_THAN -> {
                assertThat.isLessThanOrEqualTo(-1);
                assertThatInverse.isGreaterThanOrEqualTo(1);
            }
            case EQUAL -> {
                assertThat.isZero();
                assertThatInverse.isZero();
            }
        }
    }

    // the reconciler often has to process a resource without the generation changing.
    @Test
    void shouldPreserveLastTransitionTimeWhenGenerationAndStatusUnchanged() {
        Instant originalTime = TEST_TIME.instant();
        Condition originalCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime)
                .build();

        Condition newCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime.plus(1, ChronoUnit.MINUTES))
                .build();

        // when
        List<Condition> actual = ResourceState.newConditions(List.of(originalCondition), ResourceState.fromList(List.of(newCondition)));

        // then
        assertThat(actual).isEqualTo(List.of(originalCondition));
    }

    @Test
    void shouldUpdateMessageWhenGenerationAndStatusUnchanged() {
        Instant originalTime = TEST_TIME.instant();
        Condition originalCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withMessage("original message")
                .withLastTransitionTime(originalTime)
                .build();

        Condition newCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withMessage("new message")
                .withLastTransitionTime(originalTime.plus(1, ChronoUnit.MINUTES))
                .build();

        // when
        var originalConditionWithNewMessage = new ConditionBuilder(originalCondition).withMessage("new message").build();
        List<Condition> actual = ResourceState.newConditions(List.of(originalCondition), ResourceState.fromList(List.of(newCondition)));

        // then
        assertThat(actual).isEqualTo(List.of(originalConditionWithNewMessage));
    }

    // the reconciler often has to process a resource without the generation changing.
    @Test
    void shouldPreserveLastTransitionTimeWhenGenerationAndStatusChanged() {
        Instant originalTime = TEST_TIME.instant();
        Condition originalCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime)
                .build();

        Condition newCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .withLastTransitionTime(originalTime.plus(1, ChronoUnit.MINUTES))
                .build();

        // when
        List<Condition> actual = ResourceState.newConditions(List.of(originalCondition), ResourceState.fromList(List.of(newCondition)));

        // then
        assertThat(actual).isEqualTo(List.of(newCondition));
    }

    @Test
    void shouldPreserveLastTransitionTimeIfStatusUnchangedOverGenerations() {
        // given
        Instant originalTime = TEST_TIME.instant();
        Condition originalCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime)
                .build();

        Condition newCondition = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime.plus(1, ChronoUnit.MINUTES))
                .build();

        // when
        var newConditionWithOriginalTransitionTime = new ConditionBuilder(newCondition).withLastTransitionTime(originalTime).build();
        List<Condition> actual = ResourceState.newConditions(List.of(originalCondition), ResourceState.fromList(List.of(newCondition)));

        // then
        assertThat(actual).isEqualTo(List.of(newConditionWithOriginalTransitionTime));
    }

    @Test
    void shouldNotPreserveNullLastTransitionTimeIfStatusUnchangedOverGenerations() {
        // given
        Condition originalCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(null)
                .build();

        Instant newStatusTime = TEST_TIME.instant();
        Condition newCondition = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(newStatusTime)
                .build();

        // when
        List<Condition> actual = ResourceState.newConditions(List.of(originalCondition), ResourceState.fromList(List.of(newCondition)));

        // then
        assertThat(actual).isEqualTo(List.of(newCondition));
    }

    @Test
    void shouldUseLatestTransitionTimeIfStatusChangedOverGenerations() {
        // given
        Instant originalTime = TEST_TIME.instant();
        Condition originalCondition = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime)
                .build();

        Condition newCondition = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .withLastTransitionTime(originalTime.plus(1, ChronoUnit.MINUTES))
                .build();

        // when
        List<Condition> actual = ResourceState.newConditions(List.of(originalCondition), ResourceState.fromList(List.of(newCondition)));

        // then
        assertThat(actual).isEqualTo(List.of(newCondition));
    }

}
