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

/**
 * A position in the routing/filter tree, from the top-level virtual cluster down to wherever a
 * frame currently sits (or, for an internally-issued request, down to the specific filter or
 * router that issued it).
 * <p>
 * Represented as a self-recursive cons-list: the element you're holding (e.g. a response's own
 * path, or a route filter's own position) is the head, and {@link #parent()} walks back toward
 * the top-level virtual cluster, terminating at {@link ClientOrigin}. Building a child position is
 * just constructing the appropriate case with the parent element as {@code parent}.
 * Because every case is a record exact-match and ancestor-match both reduce to plain comparisons.
 * <p>
 * Only a {@link Route} or {@link ClientOrigin} may ever appear as another
 * element's {@link #parent()} (see {@link RoutePosition} ). A {@link FilterOrigin} or {@link RouterOrigin}
 * always sits directly on a route (or, for a VC-level filter, the client origin),
 * awaiting a promise. A router only ever issues an out-of-band request against a
 * concrete named route, never {@code PathElement}.
 */
public sealed interface PathElement {

    /**
     * The route this element sits on, or the next element toward the top-level virtual cluster,
     * or {@link ClientOrigin} if this is the outermost element.
     *
     * @return the parent position
     */
    RoutePosition parent();

    /**
     * True if {@code this} is {@code other}, or lies on its ancestor chain (walked via {@link #parent()}).
     *
     * @param other the element to test
     * @return {@code true} if {@code this} is an ancestor of (or the same as) {@code other}
     */
    default boolean isAncestorOfOrSameAs(PathElement other) {
        for (PathElement p = other;; p = p.parent()) {
            if (p.equals(this)) {
                return true;
            }
            if (p instanceof ClientOrigin) {
                return false;
            }
        }
    }

    /**
     * The promise to complete, if this element represents an in-flight internal (router- or
     * filter-issued) request.
     *
     * @return the promise, or empty if this element does not represent an internal request
     */
    default Optional<CompletableFuture<?>> pendingPromise() {
        return switch (this) {
            case FilterOrigin f -> Optional.of(f.promise());
            case RouterOrigin r -> Optional.of(r.promise());
            case Route ignored -> Optional.empty();
            case ClientOrigin ignored -> Optional.empty();
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
        for (PathElement p = this;; p = p.parent()) {
            if (p instanceof ClientOrigin) {
                break;
            }
            rootToLeaf.addFirst(p);
        }
        return rootToLeaf.stream()
                .map(p -> switch (p) {
                    case Route r -> r.name();
                    case FilterOrigin f -> f.name() + "[" + f.ordinal() + "]";
                    case RouterOrigin ignored -> "<router>";
                    case ClientOrigin ignored -> throw new AssertionError("unreachable: ClientOrigin is filtered out by the walk above");
                })
                .collect(Collectors.joining("/"));
    }

    /**
     * This element's own position in the route tree: for a {@link RoutePosition}, itself; for a
     * {@link FilterOrigin} or {@link RouterOrigin}, the route (or client origin) it sits on
     * ({@link #parent()}).
     *
     * @return this element's own route position
     */
    default RoutePosition routePosition() {
        return switch (this) {
            case Route r -> r;
            case FilterOrigin f -> f.parent();
            case RouterOrigin r -> r.parent();
            case ClientOrigin c -> c;
        };
    }

    /**
     * Returns a path element with {@code newPosition} grafted onto this element's own place in the tree:
     * <ul>
     * <li>for a {@link FilterOrigin} or {@link RouterOrigin}, returns a copy of this element with {@link #parent()}
     * replaced by {@code newPosition} - preserving the element's own identity (name, ordinal,
     * promise) so it can still be recognized as its issuer's own request once a route is resolved
     * beneath it;</li>
     * <li>for a {@link RoutePosition}, simply returns {@code newPosition}, since a bare route (or
     * the client origin) carries no identity beyond its position.</li>
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
            case ClientOrigin ignored -> newPosition;
        };
    }

    /**
     * A position that a {@link FilterOrigin} or {@link RouterOrigin} may sit on, or that a
     * {@link Route} may chain to as its own {@link #parent()}: either a further {@link Route}
     * toward the top-level virtual cluster, or {@link ClientOrigin}, the root. Excludes
     * {@link FilterOrigin}/{@link RouterOrigin} from ever being a parent, in turn.
     */
    sealed interface RoutePosition extends PathElement permits Route, ClientOrigin {
    }

    /**
     * One hop through a (possibly nested) router's route. Never carries a promise: this is what
     * ordinary client-forwarded traffic terminates in, and what an internally-issued request's
     * own position sits on top of.
     *
     * @param name the route's own (unqualified) name at this nesting level
     * @param parent the next element toward the top-level virtual cluster
     */
    record Route(String name, RoutePosition parent) implements RoutePosition {
        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * A specific filter instance's own position: an internally-issued request made by the filter
     * named by {@code name}, awaiting {@code promise}. Sits on the route named by {@code parent()}
     * if route-scoped, or directly on {@link ClientOrigin} if this filter applies at the
     * virtual-cluster level.
     *
     * @param name the filter's configured name
     * @param ordinal the filter's position within its route's filter list, disambiguating
     *        two filters that happen to share a name
     * @param promise the promise to complete when the response arrives
     * @param parent the route this filter is installed on, or {@link ClientOrigin} if it is
     *        installed at the virtual-cluster level
     */
    record FilterOrigin(
                        String name,
                        int ordinal,
                        CompletableFuture<?> promise,
                        RoutePosition parent)
            implements PathElement {
        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * A router's own position: an internally-issued request made by the router itself, awaiting
     * {@code promise}. {@code parent()} is always a concrete {@link Route} - routing an
     * out-of-band request always targets a named route, never the bare client origin.
     *
     * @param promise the promise to complete when the response arrives
     * @param parent the route level this router is installed at
     */
    record RouterOrigin(
                        CompletableFuture<?> promise,
                        Route parent)
            implements PathElement {
        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * The root of every path: the top-level virtual cluster, before any route or out-of-band
     * request has been layered on top.
     */
    record ClientOrigin() implements RoutePosition {

        public static final ClientOrigin INSTANCE = new ClientOrigin();

        @Override
        public RoutePosition parent() {
            return this;
        }

        @Override
        public String toString() {
            return describe();
        }
    }
}
