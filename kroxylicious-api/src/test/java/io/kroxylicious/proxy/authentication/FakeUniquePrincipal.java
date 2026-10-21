/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

@SuppressWarnings("removal") // Principal, Unique scheduled for removal
@Unique
public record FakeUniquePrincipal(String name) implements Principal {}
