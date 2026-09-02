/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A position in the routing/filter tree, from the top-level virtual cluster down to wherever a
 * frame currently sits (or, for an internally-issued request, down to the specific filter or
 * router that issued it).
 * <p>
 * Represented as a self-recursive cons-list: the element you're holding (e.g. the leaf of a
 * response's path, or a route filter's own position) is the head, and {@link #next()} walks back
 * toward the top-level virtual cluster, terminating at {@code null}. Building a child position is
 * just constructing the appropriate case with the parent element as {@code next} - there is no
 * separate builder or wrapper type. Because every case is a record, {@code equals}/{@code
 * hashCode} are structural (recursing through {@code next}), so exact-match and ancestor-match
 * both reduce to plain comparisons - no string separators, no escaping, no ambiguity about where
 * one segment ends and the next begins.
 */
public sealed interface PathElement {

    /**
     * The next element toward the top-level virtual cluster, or {@code null} if this is the
     * outermost element.
     *
     * @return the next element, or {@code null}
     */
    @Nullable
    PathElement next();

    /**
     * True if {@code this} is {@code other}, or lies on its ancestor chain (walked via {@link #next()}).
     *
     * @param other the element to test
     * @return {@code true} if {@code this} is an ancestor of (or the same as) {@code other}
     */
    default boolean isAncestorOfOrSameAs(PathElement other) {
        for (PathElement p = other; p != null; p = p.next()) {
            if (p.equals(this)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The promise to complete, if this element represents an in-flight internal (router- or
     * filter-issued) request. Named distinctly from the {@code promise()} accessor {@link Filter}
     * and {@link Router} each already expose, since a sealed interface default method can't
     * overload a record component accessor with a different return type.
     *
     * @return the promise, or empty if this element does not represent an internal request
     */
    default Optional<CompletableFuture<?>> pendingPromise() {
        return switch (this) {
            case Filter f -> Optional.of(f.promise());
            case Router r -> Optional.of(r.promise());
            case Route ignored -> Optional.empty();
        };
    }

    /**
     * A human-readable rendering of this path, root to leaf, for logging - e.g.
     * {@code "outerRoute/middleRouter/innerRoute#markerFilter[0]"}. Not used for comparison.
     *
     * @return a display string for this path
     */
    default String describe() {
        Deque<PathElement> rootToLeaf = new ArrayDeque<>();
        for (PathElement p = this; p != null; p = p.next()) {
            rootToLeaf.addFirst(p);
        }
        return rootToLeaf.stream()
                .map(p -> switch (p) {
                    case Route r -> r.name();
                    case Filter f -> f.name() + "[" + f.ordinal() + "]";
                    case Router ignored -> "<router>";
                })
                .collect(Collectors.joining("/"));
    }

    /**
     * One hop through a (possibly nested) router's route. Never carries a promise: this is what
     * ordinary client-forwarded traffic terminates in, and what out-of-band leaves sit on top of.
     *
     * @param name the route's own (unqualified) name at this nesting level
     * @param next the next element toward the top-level virtual cluster, or {@code null}
     */
    record Route(String name, @Nullable PathElement next) implements PathElement {}

    /**
     * An out-of-band leaf: a specific filter instance, on the route named by {@code next()},
     * awaiting {@code promise}.
     *
     * @param name the filter's configured name
     * @param ordinal the filter's position within its route's filter list, disambiguating
     *        two filters that happen to share a name
     * @param promise the promise to complete when the response arrives
     * @param next the route this filter is installed on
     */
    record Filter(String name, int ordinal, CompletableFuture<?> promise, @Nullable PathElement next) implements PathElement {}

    /**
     * An out-of-band leaf: the router itself, at the route level named by {@code next()},
     * awaiting {@code promise}.
     *
     * @param promise the promise to complete when the response arrives
     * @param next the route level this router is installed at
     */
    record Router(CompletableFuture<?> promise, @Nullable PathElement next) implements PathElement {}
}
