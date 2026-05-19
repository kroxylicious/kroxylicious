/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Carries the inputs and outputs for a single deprecation check pass.
 * <p>
 * Checkers call {@link #addCondition} to append a {@link Condition} to {@link #conditions()}.
 * <p>
 * After all checkers have run, the accumulated conditions can be retrieved via {@link #conditions()}.
 *
 * @param <S> the spec type of the custom resource
 * @param <T> the status type of the custom resource
 * @param <R> the custom resource type
 * @param <F> the status factory type
 *
 * @see DeprecationChecker
 * @see #addCondition(Condition.Type, String)
 */
public class DeprecationCheckContext<S, T, R extends CustomResource<S, T>, F extends StatusFactory<R>> {

    private final R resource;
    private final F statusFactory;
    private final List<Condition> conditions = new ArrayList<>();

    public DeprecationCheckContext(R resource, F statusFactory) {
        this.resource = Objects.requireNonNull(resource);
        this.statusFactory = Objects.requireNonNull(statusFactory);
    }

    /**
     * Returns the custom resource being checked.
     *
     * @return the custom resource
     */
    public R resource() {
        return resource;
    }

    /**
     * Appends a {@link Condition} to {@link #conditions()}.
     *
     * @param type the condition type (typically {@link Condition.Type#DeprecationWarning})
     * @param message the human-readable message that is attached to the condition
     */
    public void addCondition(Condition.Type type, String message) {
        conditions.add(statusFactory.newTrueCondition(resource, type, message));
    }

    /**
     * Returns an unmodifiable view of the conditions accumulated by checkers so far.
     *
     * @return accumulated conditions
     */
    public List<Condition> conditions() {
        return List.copyOf(conditions);
    }
}