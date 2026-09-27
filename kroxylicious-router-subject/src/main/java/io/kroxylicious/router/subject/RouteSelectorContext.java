/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.Set;

/**
 * Read-only context handed to a {@link RouteSelector}.
 */
public interface RouteSelectorContext {

    /**
     * Returns the route names declared on the owning router.
     *
     * @return the route names declared on the owning router
     */
    Set<String> routeNames();
}
