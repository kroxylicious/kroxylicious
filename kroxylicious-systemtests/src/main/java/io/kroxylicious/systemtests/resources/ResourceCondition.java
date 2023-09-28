/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources;

import java.util.Objects;
import java.util.function.Predicate;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * The type Resource condition.
 *
 * @param <T>  the type parameter
 */
public class ResourceCondition<T extends HasMetadata> {
    private final Predicate<T> predicate;
    private final String conditionName;

    /**
     * Instantiates a new Resource condition.
     *
     * @param predicate the predicate
     * @param conditionName the condition name
     */
    public ResourceCondition(Predicate<T> predicate, String conditionName) {
        this.predicate = predicate;
        this.conditionName = conditionName;
    }

    /**
     * Gets condition name.
     *
     * @return the condition name
     */
    public String getConditionName() {
        return conditionName;
    }

    /**
     * Gets predicate.
     *
     * @return the predicate
     */
    public Predicate<T> getPredicate() {
        return predicate;
    }

    /**
     * Readiness resource condition.
     *
     * @param <T>  the type parameter
     * @param type the type
     * @return the resource condition
     */
    public static <T extends HasMetadata> ResourceCondition<T> readiness(ResourceType<T> type) {
        return new ResourceCondition<>(type::waitForReadiness, "readiness");
    }

    /**
     * Deletion resource condition.
     *
     * @param <T>  the type parameter
     * @return the resource condition
     */
    public static <T extends HasMetadata> ResourceCondition<T> deletion() {
        return new ResourceCondition<>(Objects::isNull, "deletion");
    }
}
