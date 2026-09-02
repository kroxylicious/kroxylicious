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
 * Represented as a self-recursive cons-list: the element you're holding (e.g. a response's own
 * path, or a route filter's own position) is the head, and {@link #parent()} walks back toward
 * the top-level virtual cluster, terminating at {@code null}. Building a child position is just
 * constructing the appropriate case with the parent element as {@code parent}.
 * Because every case is a record, {@code equals}/{@code
 * hashCode} are structural (recursing through {@code parent}), so exact-match and ancestor-match
 * both reduce to plain comparisons.
 * <p>
 * Only {@link Route} may ever appear as another element's {@link #parent()}: a {@link
 * FilterOrigin} or {@link RouterOrigin} always sits directly on a route, awaiting a promise -
 * nothing is ever layered on top of one of these in turn. This is enforced by every
 * constructor's {@code parent} parameter being typed {@link Route}, not {@code PathElement}.
 */
public sealed interface PathElement {

    /**
     * The route this element sits on, or the next element toward the top-level virtual cluster,
     * or {@code null} if this is the outermost element.
     *
     * @return the parent route, or {@code null}
     */
    @Nullable
    Route parent();

    /**
     * True if {@code this} is {@code other}, or lies on its ancestor chain (walked via {@link #parent()}).
     *
     * @param other the element to test
     * @return {@code true} if {@code this} is an ancestor of (or the same as) {@code other}
     */
    default boolean isAncestorOfOrSameAs(PathElement other) {
        for (PathElement p = other; p != null; p = p.parent()) {
            if (p.equals(this)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The promise to complete, if this element represents an in-flight internal (router- or
     * filter-issued) request. Named distinctly from the {@code promise()} accessor {@link
     * FilterOrigin} and {@link RouterOrigin} each already expose, since a sealed interface default
     * method can't overload a record component accessor with a different return type.
     *
     * @return the promise, or empty if this element does not represent an internal request
     */
    default Optional<CompletableFuture<?>> pendingPromise() {
        return switch (this) {
            case FilterOrigin f -> Optional.of(f.promise());
            case RouterOrigin r -> Optional.of(r.promise());
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
        for (PathElement p = this; p != null; p = p.parent()) {
            rootToLeaf.addFirst(p);
        }
        return rootToLeaf.stream()
                .map(p -> switch (p) {
                    case Route r -> r.name();
                    case FilterOrigin f -> f.name() + "[" + f.ordinal() + "]";
                    case RouterOrigin ignored -> "<router>";
                })
                .collect(Collectors.joining("/"));
    }

    /**
     * This element's own position in the route tree: for a {@link Route}, itself; for a {@link
     * FilterOrigin} or {@link RouterOrigin}, the route it sits on ({@link #parent()}). Replaces
     * the "strip the out-of-band leaf to get the route" logic that would otherwise need to be
     * duplicated at each call site that needs a frame's route position regardless of whether a
     * filter/router happens to be awaiting a promise on top of it.
     *
     * @return this element's own route position, or {@code null} if it is a bare top-level element
     */
    default @Nullable Route routePosition() {
        return switch (this) {
            case Route r -> r;
            case FilterOrigin f -> f.parent();
            case RouterOrigin r -> r.parent();
        };
    }

    /**
     * Returns a path element with {@code newPosition} grafted onto this element's own place in the tree:
     * <ul>
     * <li>for a {@link FilterOrigin} or {@link RouterOrigin}, returns a copy of this element with {@link #parent()}
     * replaced by {@code newPosition} - preserving the element's own identity (name, ordinal,
     * promise) so it can still be recognized as its issuer's own request once a route is resolved
     * beneath it;</li>
     * <li>for a {@link Route}, simply returns {@code newPosition}, since a bare route
     * carries no identity beyond its position.</li>
     * </ul>
     * <p>Callers must handle a {@code null} original path
     * themselves, since this is an instance method and {@code this} can never be null.
     * </p>
     * @param newPosition the resolved route to graft onto this element's own position
     * @return the grafted path element
     */
    default PathElement graft(Route newPosition) {
        return switch (this) {
            case FilterOrigin f -> new FilterOrigin(f.name(), f.ordinal(), f.promise(), newPosition);
            case RouterOrigin r -> new RouterOrigin(r.promise(), newPosition);
            case Route ignored -> newPosition;
        };
    }

    /**
     * One hop through a (possibly nested) router's route. Never carries a promise: this is what
     * ordinary client-forwarded traffic terminates in, and what an internally-issued request's
     * own position sits on top of.
     *
     * @param name the route's own (unqualified) name at this nesting level
     * @param parent the next element toward the top-level virtual cluster, or {@code null}
     */
    record Route(String name, @Nullable Route parent) implements PathElement {
        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * A specific filter instance's own position: an internally-issued request made by the filter
     * named by {@code name}, on the route named by {@code parent()}, awaiting {@code promise}.
     *
     * @param name the filter's configured name
     * @param ordinal the filter's position within its route's filter list, disambiguating
     *        two filters that happen to share a name
     * @param promise the promise to complete when the response arrives
     * @param parent the route this filter is installed on
     */
    record FilterOrigin(
            String name,
            int ordinal,
            CompletableFuture<?> promise,
            @Nullable Route parent
    ) implements PathElement {
        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * A router's own position: an internally-issued request made by the router itself, at the
     * route level named by {@code parent()}, awaiting {@code promise}.
     *
     * @param promise the promise to complete when the response arrives
     * @param parent the route level this router is installed at
     */
    record RouterOrigin(
            CompletableFuture<?> promise,
            @Nullable Route parent
    ) implements PathElement {
        @Override
        public String toString() {
            return describe();
        }
    }
}
