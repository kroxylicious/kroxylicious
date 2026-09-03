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

/**
 * Where a {@link Frame} sits, or was last known to sit, in a virtual cluster's routing/filter
 * tree - see {@link Frame#routing()}. This is one of two orthogonal things:
 * <ul>
 * <li>a {@link RoutePosition}: a pure address in the tree, nothing more; or</li>
 * <li>an {@link Originator}: the identity of a filter- or router-issued out-of-band (OOB) request
 * that is still awaiting its response, anchored at the {@link RoutePosition} it was issued
 * from.</li>
 * </ul>
 * <p>
 * There is exactly one {@code RoutePosition} tree per routing scope (the top-level virtual
 * cluster, or a nested router's own routes), built once, statically, from configuration, when
 * that scope's pipeline handlers are constructed - not accumulated frame-by-frame as a request is
 * forwarded. Every {@code RoutePosition} in it, however deeply nested, has an ancestor chain
 * (via {@link RoutePosition#parent()}) that terminates at {@link ClientOrigin}, the scope's root.
 * This single shared tree is what lets route-scoped filters and OOB response matching (see
 * {@code RouteFilterHandler}, {@code FilterHandler#isRecipient}, {@code RouteDispatcher#handleResponse})
 * decide "does this frame belong to my route?" for client-forwarded and OOB traffic alike, via one
 * structural ancestor-chain comparison ({@link RoutePosition#isAncestorOfOrSameAs}) - which is why
 * an {@link Originator} is always anchored <em>onto</em> this shared tree (via
 * {@link Originator#position()}) rather than being the root of some separate lineage of its own:
 * a sibling route filter could never recognise OOB traffic as belonging to its route otherwise.
 * <p>
 * A frame's {@link Frame#routing()} value is set wholesale at specific points in its lifecycle
 * (route resolution, OOB issuance, response restoration) - see {@link Frame} for the full
 * sequence - never built up by consing a new head on as the frame moves through each pipeline
 * handler. In particular, a response's routing value is either copied verbatim from its request or
 * reset to a specific already-known value; it is never reconstructed by walking {@code parent()}
 * backwards. Preserving a request's routing value across a real broker round trip is a separate,
 * wire-level concern (see {@code CorrelationManager}), since the broker itself cannot carry proxy
 * metadata - it is not part of this type's own machinery.
 */
public sealed interface PathElement permits PathElement.RoutePosition, PathElement.Originator {

    /**
     * This element's own address in the route tree: for a {@link RoutePosition}, itself; for an
     * {@link Originator}, the position it is anchored at ({@link Originator#position()}).
     *
     * @return this element's own route position
     */
    RoutePosition routePosition();

    /**
     * The promise to complete, if this element is an {@link Originator} representing an in-flight
     * OOB request.
     *
     * @return the promise, or empty if this element is a bare {@link RoutePosition}
     */
    default Optional<CompletableFuture<?>> pendingPromise() {
        return this instanceof Originator originator ? Optional.of(originator.promise()) : Optional.empty();
    }

    /**
     * Returns a path element with {@code newPosition} grafted onto this element's own place in the
     * tree: for a {@link RoutePosition}, simply returns {@code newPosition}, since a bare position
     * carries no identity beyond its place in the tree; for an {@link Originator}, returns a copy
     * with its {@link Originator#position()} replaced by {@code newPosition}
     * ({@link Originator#reposition}) - preserving the issuer's own identity (name, ordinal,
     * promise) so it can still be recognised as its issuer's own request once a route is resolved
     * beneath it.
     * <p>
     * Callers must handle a {@code null} original routing value themselves, since this is an
     * instance method and {@code this} can never be null.
     *
     * @param newPosition the resolved route to graft onto this element's own position
     * @return the grafted path element
     */
    default PathElement graft(Route newPosition) {
        return switch (this) {
            case RoutePosition ignored -> newPosition;
            case Originator originator -> originator.reposition(newPosition);
        };
    }

    /**
     * A human-readable rendering of this path, root to leaf, for logging - e.g.
     * {@code "outerRoute/middleRouter/innerRoute#markerFilter[0]"}. Not used for comparison.
     *
     * @return a display string for this path
     */
    default String describe() {
        Deque<String> rootToLeaf = new ArrayDeque<>();
        for (RoutePosition p = routePosition();; p = p.parent()) {
            if (p instanceof ClientOrigin) {
                break;
            }
            rootToLeaf.addFirst(((Route) p).name());
        }
        switch (this) {
            case FilterOriginator f -> rootToLeaf.addLast(f.name() + "[" + f.ordinal() + "]");
            case RouterOriginator ignored -> rootToLeaf.addLast("<router>");
            case RoutePosition ignored -> {
                // no originator identity to append - the position itself is the whole path
            }
        }
        return String.join("/", rootToLeaf);
    }

    /**
     * A pure address in the (single, statically-built, per-routing-scope) route tree: either a
     * further {@link Route} toward the top-level virtual cluster, or {@link ClientOrigin}, the
     * root. Never carries a promise or any issuer identity - see {@link Originator} for that.
     */
    sealed interface RoutePosition extends PathElement permits Route, ClientOrigin {

        /**
         * The next position toward the top-level virtual cluster, or {@code this} if this is
         * already {@link ClientOrigin}.
         *
         * @return the parent position
         */
        RoutePosition parent();

        @Override
        default RoutePosition routePosition() {
            return this;
        }

        /**
         * True if {@code this} is {@code other}'s own route position ({@link PathElement#routePosition()}),
         * or lies on its ancestor chain (walked via {@link #parent()}).
         *
         * @param other the element to test
         * @return {@code true} if {@code this} is an ancestor of (or the same as) {@code other}'s
         *         route position
         */
        default boolean isAncestorOfOrSameAs(PathElement other) {
            for (RoutePosition p = other.routePosition();; p = p.parent()) {
                if (p.equals(this)) {
                    return true;
                }
                if (p instanceof ClientOrigin) {
                    return false;
                }
            }
        }
    }

    /**
     * One hop through a (possibly nested) router's route. Never carries a promise or issuer
     * identity: this is what ordinary client-forwarded traffic terminates in, and what an
     * {@link Originator} sits anchored on top of.
     *
     * @param name the route's own (unqualified) name at this nesting level
     * @param parent the next position toward the top-level virtual cluster
     */
    record Route(String name, RoutePosition parent) implements RoutePosition {
        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * The root of every {@link RoutePosition} tree: the top-level virtual cluster (or, for a
     * nested router's own tree, that router's activation point), before any route has been
     * layered on top.
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

    /**
     * The identity of an in-flight, internally-issued (filter- or router-originated) OOB request:
     * who issued it, the promise to complete once its response arrives, and the
     * {@link RoutePosition} it is currently anchored at. Unlike a {@link RoutePosition}, an
     * {@code Originator} is never itself a position other elements build upon - it is always the
     * outermost fact about a frame, layered on top of the shared route tree, not a link in it.
     */
    sealed interface Originator extends PathElement permits FilterOriginator, RouterOriginator {

        /**
         * The promise to complete when this OOB request's response arrives.
         *
         * @return the pending promise
         */
        @SuppressWarnings("java:S1452")
        CompletableFuture<?> promise();

        /**
         * The route position this OOB request is currently anchored at: the issuer's own route
         * (or, for a VC-level filter, {@link ClientOrigin}), possibly deepened by
         * {@link #reposition} once a target route is resolved.
         *
         * @return the anchoring route position
         */
        RoutePosition position();

        @Override
        default RoutePosition routePosition() {
            return position();
        }

        /**
         * Returns a copy of this originator anchored at {@code newPosition} instead, preserving
         * its own identity (name, ordinal, promise) - used by {@link PathElement#graft} once a
         * route is resolved beneath an originator that didn't yet have one.
         *
         * @param newPosition the resolved route to anchor this originator at
         * @return the repositioned originator
         */
        Originator reposition(Route newPosition);
    }

    /**
     * A specific filter instance's own identity: an internally-issued request made by the filter
     * named by {@code name}, awaiting {@code promise}. Anchored at the route named by
     * {@code position()} if route-scoped, or directly at {@link ClientOrigin} if this filter
     * applies at the virtual-cluster level.
     *
     * @param name the filter's configured name
     * @param ordinal the filter's position within its route's filter list, disambiguating
     *        two filters that happen to share a name
     * @param promise the promise to complete when the response arrives
     * @param position the route this filter is installed on, or {@link ClientOrigin} if it is
     *        installed at the virtual-cluster level
     */
    record FilterOriginator(
                            String name,
                            int ordinal,
                            CompletableFuture<?> promise,
                            RoutePosition position)
            implements Originator {
        @Override
        public Originator reposition(Route newPosition) {
            return new FilterOriginator(name, ordinal, promise, newPosition);
        }

        @Override
        public String toString() {
            return describe();
        }
    }

    /**
     * A router's own identity: an internally-issued request made by the router itself, awaiting
     * {@code promise}. {@code position()} is always a concrete {@link Route} - routing an
     * out-of-band request always targets a named route, never the bare client origin.
     *
     * @param promise the promise to complete when the response arrives
     * @param position the route level this router is installed at
     */
    record RouterOriginator(
                            CompletableFuture<?> promise,
                            Route position)
            implements Originator {
        @Override
        public Originator reposition(Route newPosition) {
            return new RouterOriginator(promise, newPosition);
        }

        @Override
        public String toString() {
            return describe();
        }
    }
}
