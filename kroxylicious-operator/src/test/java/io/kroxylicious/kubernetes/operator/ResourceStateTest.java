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

    Clock TEST_TIME = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

    @Test
    void shouldReturnConditionWithLargerGeneration() {
        Condition c12 = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .build();

        Condition c13 = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .build();

        Condition c13True = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .build();

        assertThat(ResourceState.newConditions(List.of(c12), new ResourceState(c13))).isEqualTo(List.of(c13));
        assertThat(ResourceState.newConditions(List.of(c13), new ResourceState(c12))).isEqualTo(List.of(c13));

        assertThat(ResourceState.newConditions(List.of(c12), new ResourceState(c13True))).isEqualTo(List.of());
        assertThat(ResourceState.newConditions(List.of(c13True), new ResourceState(c12))).isEqualTo(List.of());
    }

    @Test
    void shouldPreserveLastTransitionTime() {
        Instant originalTime = TEST_TIME.instant();
        Condition c12 = new ConditionBuilder()
                .withObservedGeneration(12L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime)
                .build();

        Condition c13 = new ConditionBuilder()
                .withObservedGeneration(13L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withLastTransitionTime(originalTime.plus(1, ChronoUnit.MINUTES))
                .build();

        var c13WithOriginalTime = new ConditionBuilder(c13).withLastTransitionTime(originalTime).build();

        assertThat(ResourceState.newConditions(List.of(c12), new ResourceState(c13))).isEqualTo(List.of(c13WithOriginalTime));
        // Let's not retrospectively change the ltt on an existing condition
        assertThat(ResourceState.newConditions(List.of(c13), new ResourceState(c12))).isEqualTo(List.of(c13));
    }

}
