/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.authentication.Subject;

/**
 * Represents the outcome of a call to {@link Authorizer#authorize(io.kroxylicious.proxy.authentication.Subject, List)}
 * @param allowed The allowed actions.
 * @param denied The denied actions.
 */
public record AuthorizeResult(
                              Subject subject,
                              List<Action> allowed,
                              List<Action> denied) {

    public AuthorizeResult {
        Objects.requireNonNull(allowed);
        Objects.requireNonNull(denied);
    }

    /**
     * Returns a list of the names of all the resources which the {@link #subject()}
     * is allowed to perform the given {@code operation} on.
     * @param operation The operation.
     * @return The names of the allowed resources.
     */
    public List<String> allowed(ResourceType<?> operation) {
        return allowed().stream()
                .filter(a -> a.operation().equals(operation))
                .map(Action::resourceName)
                .toList();
    }

    /**
     * Returns a list of the names of all the resources which the {@link #subject()}
     * is denied from performing the given {@code operation} on.
     * @param operation The operation.
     * @return The names of the denied resources.
     */
    public List<String> denied(ResourceType<?> operation) {
        return denied().stream()
                .filter(a -> a.operation().equals(operation))
                .map(Action::resourceName)
                .toList();
    }

    /**
     * Returns the decision about whether the given {@link #subject()} is allowed to perform the given
     * {@code operation} on the resource with the given {@code resourceName}.
     * @param operation The operation that would be performed on the resource.
     * @param resourceName The name of the resource that the operation would be performed on.
     * @return The decision.
     */
    public Decision decision(ResourceType<?> operation, String resourceName) {
        return allowed().stream()
                .anyMatch(a -> a.operation().equals(operation)
                        && a.resourceName().equals(resourceName)) ? Decision.ALLOW : Decision.DENY;
    }

    /**
     * Partitions the given {@code items}, whose names can be obtained via the given {@code toName} function, into two lists
     * based on whether the {@link #subject()} is allowed to perform the given {@code operation} on them.
     * @param items The items to partition
     * @param operation The operation
     * @param toName A function that returns the name of each item.
     * @return A pair of lists of the items which the subject is allowed to, or denied from, performing the operation on.
     * It is guaranteed that there is always an entry for both {@code ALLOW} and {@code DENY} in the returned map.
     * @param <T> The type of item.
     */
    public <T> Map<Decision, List<T>> partition(Collection<T> items, ResourceType<?> operation, Function<T, String> toName) {
        HashMap<Decision, List<T>> byDecision = items.stream().collect(Collectors.groupingBy(
                item -> decision(operation, toName.apply(item)),
                HashMap::new,
                Collectors.toList()));
        byDecision.putIfAbsent(Decision.ALLOW, List.of());
        byDecision.putIfAbsent(Decision.DENY, List.of());
        return byDecision;
    }

}
