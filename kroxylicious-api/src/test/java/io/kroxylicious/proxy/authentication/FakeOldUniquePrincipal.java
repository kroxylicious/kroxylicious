/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

@SuppressWarnings("deprecation")
@Unique
public record FakeOldUniquePrincipal(String name) implements Principal {}
