/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ResourceState {

    public static final Comparator<Condition> STATE_TRANSITION_COMPARATOR = Comparator.comparing(Condition::getMessage, Comparator.nullsLast(String::compareTo))
            .thenComparing(Condition::getReason, Comparator.nullsLast(String::compareTo))
            .thenComparing(Condition::getStatus, Comparator.nullsLast(Condition.Status::compareTo))
            .thenComparing(Condition::getType, Comparator.nullsLast(Condition.Type::compareTo));

    private final SortedMap<Condition.Type, Condition> conditions;

    ResourceState(Map<Condition.Type, Condition> conditions) {
        Objects.requireNonNull(conditions, "conditions cannot be null");
        this.conditions = new TreeMap<>(conditions);
    }

    static ResourceState of(Condition condition) {
        return new ResourceState(Map.of(condition.getType(), condition));
    }

    /**
     * Get a Conditions from the list of conditions on a CR's status.
     * @param conditionsList Conditions from the list of conditions on a CR's status.
     * @return An optional conditions object
     */
    static ResourceState fromList(List<Condition> conditionsList) {
        // Belt+braces: There _should_ be at most one such condition, but we assume there's more than one
        // we pick the condition with the largest observedGeneration (there's on point keeping old conditions around)
        // then we prefer Unknown over False over True statuses
        // finally we compare the last transition time, though this is only serialized with second resolution
        // and there's no guarantee that they call came from the same clock.
        Map<Condition.Type, List<Condition>> collect = conditionsList.stream().collect(Collectors.groupingBy(Condition::getType));
        Map<Condition.Type, Condition> collect1 = collect.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue().size() > 0)
                .collect(Collectors.toMap(entry -> entry.getKey(),
                        entry -> entry.getValue().stream()
                                .max(Comparator.comparing(Condition::getObservedGeneration, Comparator.nullsFirst(Long::compareTo))
                                        .thenComparing(Condition::getStatus, Comparator.nullsFirst(Condition.Status::compareTo)).reversed()
                                        .thenComparing(Condition::getLastTransitionTime, Comparator.nullsFirst(Instant::compareTo)))
                                .get()));
        return new ResourceState(collect1);
    }

    public ResourceState replacementFor(ResourceState existingState) {
        return new ResourceState(Stream.concat(this.conditions.values().stream(), existingState.conditions.values().stream())
                .collect(Collectors.toMap(Condition::getType, Function.identity(),
                        (c1, c2) -> {
                            if (c1.getObservedGeneration() == null) {
                                return c2;
                            }
                            if (c2.getObservedGeneration() == null) {
                                return c1;
                            }
                            else if (Objects.equals(c1.getObservedGeneration(), c2.getObservedGeneration())) {
                                if (Objects.equals(c1.getStatus(), c2.getStatus())) {
                                    return earliest(c1, c2);
                                }
                                else {
                                    return mostRecent(c1, c2);
                                }
                            }
                            else {
                                if (c1.getObservedGeneration() > c2.getObservedGeneration()) {
                                    c1.setLastTransitionTime(minTransitionTime(c1, c2));
                                    return c1;
                                }
                                else {
                                    c2.setLastTransitionTime(minTransitionTime(c1, c2));
                                    return c2;
                                }
                            }
                        },
                        TreeMap::new)));

    }

    private Instant minTransitionTime(Condition conditionA, Condition conditionB) {
        if (conditionA.getLastTransitionTime() == null) {
            return conditionB.getLastTransitionTime();
        }
        else if (conditionB.getLastTransitionTime() == null) {
            return conditionA.getLastTransitionTime();
        }
        else if (conditionA.getLastTransitionTime().isBefore(conditionB.getLastTransitionTime())) {
            return conditionA.getLastTransitionTime();
        }
        else {
            return conditionB.getLastTransitionTime();
        }
    }

    private static @NonNull Condition mostRecent(Condition c1, Condition c2) {
        if (c1.getLastTransitionTime() == null) {
            return c2;
        }
        else if (c2.getLastTransitionTime() == null) {
            return c1;
        }
        else if (c1.getLastTransitionTime().isAfter(c2.getLastTransitionTime())) {
            return c1;
        }
        else {
            return c2;
        }
    }

    private static @NonNull Condition earliest(Condition c1, Condition c2) {
        if (c1.getLastTransitionTime() == null) {
            return c2;
        }
        else if (c2.getLastTransitionTime() == null) {
            return c1;
        }
        else if (c1.getLastTransitionTime().isBefore(c2.getLastTransitionTime())) {
            return c1;
        }
        else {
            return c2;
        }
    }

    public List<Condition> toList() {
        return conditions.entrySet().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<Condition> newConditions(List<Condition> oldConditions, ResourceState newStatus) {
        ResourceState existingConditions = fromList(oldConditions);
        ResourceState replacement = newStatus.replacementFor(existingConditions);

        // if (existingConditions.isPresent()) {
        // // If the two conditions are the same except for observedGeneration
        // // and lastTransitionTime then update the new condition's lastTransitionTime
        // // because it doesn't really represent a state transition
        // var existing = existingConditions.get();
        // if (STATE_TRANSITION_COMPARATOR.compare(existing.conditions, replacement.conditions) == 0) {
        // @Nullable
        // Instant i1 = existing.conditions.getLastTransitionTime();
        // @Nullable
        // Instant i2 = replacement.conditions.getLastTransitionTime();
        // Instant min = Comparator.nullsLast(Instant::compareTo).compare(i1, i2) < 0 ? i1 : i2;
        // replacement = new ResourceState(, new ConditionBuilder(replacement.conditions)
        // .withLastTransitionTime(min).build());
        // }
        // }
        return replacement.toList();
    }

}
