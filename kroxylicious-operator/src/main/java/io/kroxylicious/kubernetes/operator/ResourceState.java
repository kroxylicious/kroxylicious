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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static java.util.function.Function.identity;

public class ResourceState {

    // we are aiming for a deterministic ordering, so we order by status if observed generation and last transition time are equal
    @VisibleForTesting
    static final Comparator<Condition> FRESHEST_CONDITION = Comparator.comparing(Condition::getObservedGeneration, Comparator.nullsFirst(Long::compareTo))
            .thenComparing(Condition::getLastTransitionTime, Comparator.nullsFirst(Instant::compareTo))
            .thenComparing(Condition::getStatus, Comparator.nullsFirst(Condition.Status::compareTo));

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
        // then we compare the last transition time, there's no guarantee that they call came from the same clock
        // finally we prefer Unknown over False over True statuses
        Map<Condition.Type, List<Condition>> collect = conditionsList.stream().collect(Collectors.groupingBy(Condition::getType));
        Map<Condition.Type, Condition> freshestConditionPerType = collect.entrySet().stream()
                .filter(entry -> entry.getKey() != null && !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                                .max(FRESHEST_CONDITION)
                                .orElseThrow()));
        return new ResourceState(freshestConditionPerType);
    }

    public ResourceState replacementFor(ResourceState existingState) {
        Stream<Condition> allConditions = Stream.concat(this.conditions.values().stream(), existingState.conditions.values().stream());
        return new ResourceState(allConditions.collect(Collectors.toMap(Condition::getType, identity(), this::buildNewCondition, TreeMap::new)));
    }

    Condition buildNewCondition(Condition c1, Condition c2) {
        Condition freshest = FRESHEST_CONDITION.compare(c1, c2) < 0 ? c2 : c1;
        Condition leastFresh = c1 == freshest ? c2 : c1;
        ConditionBuilder builder = freshest.edit();
        if (Objects.equals(freshest.getStatus(), leastFresh.getStatus()) && leastFresh.getLastTransitionTime() != null) {
            // retain older transition time if the status is unchanged
            builder.withLastTransitionTime(leastFresh.getLastTransitionTime());
        }
        return builder.build();
    }

    public List<Condition> toList() {
        return conditions.values().stream().toList();
    }

    static List<Condition> newConditions(List<Condition> oldConditions, ResourceState newStatus) {
        ResourceState existingConditions = fromList(oldConditions);
        ResourceState replacement = newStatus.replacementFor(existingConditions);
        return replacement.toList();
    }

}
