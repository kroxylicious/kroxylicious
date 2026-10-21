/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import io.kroxylicious.proxy.authentication.Principal;

@SuppressWarnings("removal")
// Suppress removal warnings for deprecated Principal API
public record Unit(String name) implements Principal {}
