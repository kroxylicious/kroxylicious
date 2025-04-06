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

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;

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
    void shouldPreserveLastTransitionTime() {
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

}
