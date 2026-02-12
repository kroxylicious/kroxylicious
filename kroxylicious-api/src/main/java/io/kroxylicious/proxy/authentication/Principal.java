/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * <p>An identifier held by a {@link Subject}.</p>
 *
 * <p>Implementations <strong>must</strong> override {@code hashCode()} and {@code equals(Object)} such
 * instances are equal if, and only if, they have the same implementation class and their names that are the same
 * (according to {@code equals()}). One easy way to achieve this is to use a {@code record} class with a single {@code name} component.</p>
 */
public interface Principal {
    String name();
}
