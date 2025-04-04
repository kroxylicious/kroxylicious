/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ResourceState {

    public static final Comparator<Condition> STATE_TRANSITION_COMPARATOR = Comparator.comparing(Condition::getMessage, Comparator.nullsLast(String::compareTo))
            .thenComparing(Condition::getReason, Comparator.nullsLast(String::compareTo))
            .thenComparing(Condition::getStatus, Comparator.nullsLast(Condition.Status::compareTo))
            .thenComparing(Condition::getType, Comparator.nullsLast(Condition.Type::compareTo));

    private final Condition condition;

    ResourceState(Condition condition) {
        Objects.requireNonNull(condition, "condition cannot be null");
        this.condition = condition;
    }

    /**
     * Get a Conditions from the list of conditions on a CR's status.
     * @param conditionsList Conditions from the list of conditions on a CR's status.
     * @return An optional conditions object
     */
    static Optional<ResourceState> fromList(List<Condition> conditionsList) {
        // Belt+braces: There _should_ be at most one such condition, but we assume there's more than one
        // we pick the condition with the largest observedGeneration (there's on point keeping old conditions around)
        // then we prefer Unknown over False over True statuses
        // finally we compare the last transition time, though this is only serialized with second resolution
        // and there's no guarantee that they call came from the same clock.
        return conditionsList.stream()
                .max(Comparator.comparing(Condition::getObservedGeneration)
                        .thenComparing(Condition::getStatus).reversed()
                        .thenComparing(Condition::getLastTransitionTime))
                .map(ResourceState::new);
    }

    public ResourceState replacementFor(Optional<ResourceState> existingStatus) {
        var condition = this.condition;
        return existingStatus.map(existing -> {
            if (condition.getObservedGeneration() == null) {
                return existing;
            }
            if (existing.condition.getObservedGeneration() == null) {
                return this;
            }
            if (condition.getObservedGeneration() >= existing.condition.getObservedGeneration()) {
                return this;
            }
            else {
                return existing;
            }
        }).orElse(new ResourceState(condition));
    }

    public List<Condition> toList() {
        return List.of(condition);
    }

    @VisibleForTesting
    static List<Condition> newConditions(List<Condition> oldConditions, ResourceState newStatus) {
        Optional<ResourceState> existingConditions = fromList(oldConditions);
        ResourceState replacement = newStatus.replacementFor(existingConditions);

        if (Condition.Status.TRUE == replacement.condition.getStatus()) {
            // True is the default status, so if the new condition would be True then return the empty list.
            return List.of();
        }
        if (existingConditions.isPresent()) {
            // If the two conditions are the same except for observedGeneration
            // and lastTransitionTime then update the new condition's lastTransitionTime
            // because it doesn't really represent a state transition
            var existing = existingConditions.get();
            if (STATE_TRANSITION_COMPARATOR.compare(existing.condition, replacement.condition) == 0) {
                @Nullable
                Instant i1 = existing.condition.getLastTransitionTime();
                @Nullable
                Instant i2 = replacement.condition.getLastTransitionTime();
                Instant min = Comparator.nullsLast(Instant::compareTo).compare(i1, i2) < 0 ? i1 : i2;
                replacement = new ResourceState(new ConditionBuilder(replacement.condition)
                        .withLastTransitionTime(min).build());
            }
        }
        return replacement.toList();
    }

}
