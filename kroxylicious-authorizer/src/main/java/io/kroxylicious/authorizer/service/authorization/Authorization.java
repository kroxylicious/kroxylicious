/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service.authorization;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the outcome of a call to {@link Authorizer#authorize(Subject, List)}
 * @param allowed The allowed actions.
 * @param denied The denied actions.
 */
public record Authorization(
        List<Action> allowed,
        List<Action> denied) {

    public Authorization {
        Objects.requireNonNull(allowed);
        Objects.requireNonNull(denied);
    }

    public List<String> allowed(Operation<?> operation) {
        return allowed().stream()
                .filter(a -> a.operation().equals(operation))
                .map(Action::resourceName)
                .toList();
    }

    public List<String> denied(Operation<?> operation) {
        return denied().stream()
                .filter(a -> a.operation().equals(operation))
                .map(Action::resourceName)
                .toList();
    }

    public Decision decision(Operation<?> operation, String resourceName) {
        return allowed().stream()
                .anyMatch(a -> a.operation().equals(operation)
                        && a.resourceName().equals(resourceName)) ?
                Decision.ALLOW : Decision.DENY;
    }

    public <T> Map<Decision, List<T>> partition(Collection<T> items, Operation<?> operation, Function<T, String> toName) {
        return items.stream().collect(Collectors.groupingBy(item -> decision(operation, toName.apply(item))));
    }

}
