/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.Set;

record RouteSelectorContextImpl(Set<String> routeNames) implements RouteSelectorContext {}
