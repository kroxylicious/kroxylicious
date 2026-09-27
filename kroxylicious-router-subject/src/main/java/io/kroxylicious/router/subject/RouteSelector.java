/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authentication.Subject;

/**
 * Selects the route to use for an authenticated {@link Subject}.
 *
 * <p>{@code RouteSelector} is a nested plugin type: concrete implementations
 * are annotated with {@code @Plugin} and discovered via
 * {@link io.kroxylicious.proxy.router.RouterFactoryContext#pluginInstance}, which
 * constructs them with a no-arg constructor. {@link #initialize(Object)} is
 * called exactly once afterwards, before any call to {@link #selectRoute}, to
 * supply the plugin's configuration.</p>
 *
 * @param <C> the configuration type, deserialized from the router's
 *           {@code selectorConfig} property. Use {@link Void} if not configurable.
 */
public interface RouteSelector<C> {

    /**
     * Initializes the selector with the given configuration.
     *
     * <p>Called exactly once, before any call to {@link #selectRoute}.</p>
     *
     * @param config the selector configuration
     */
    void initialize(C config);

    /**
     * Selects the route for the given subject.
     *
     * @param subject the authenticated subject
     * @param context read-only context describing the owning router
     * @return the route to use, or {@link Optional#empty()} to reject (fail-closed)
     */
    @SuppressWarnings({ "java:S5738", "removal" })
    CompletionStage<Optional<String>> selectRoute(Subject subject, RouteSelectorContext context);

    /**
     * The exhaustive set of route names this selector may return, for startup validation
     * against the router's declared routes.
     *
     * <ul>
     *   <li>{@code Optional.empty()} (the default) — the selector routes dynamically and
     *       cannot enumerate its routes ahead of time; the router skips route-existence
     *       validation for this selector.</li>
     *   <li>a present set — the exhaustive list of routes the selector may return; the
     *       router fails startup if any is not a declared route.</li>
     * </ul>
     *
     * <p>Returns a {@link CompletionStage} to mirror {@link #selectRoute}, but under a
     * sync-only contract: implementations must complete the stage synchronously
     * (return an already-completed stage). The router resolves it at startup.</p>
     *
     * @return the exhaustive set of routes this selector may return, or empty if unknown ahead of time
     */
    default CompletionStage<Optional<Set<String>>> referencedRoutes() {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
