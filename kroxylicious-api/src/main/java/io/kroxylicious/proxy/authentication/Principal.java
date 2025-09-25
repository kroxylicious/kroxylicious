/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * An identifier held by a {@link Subject}.
 */
public interface Principal {
    // TODO this should not really be an interface, since we depend on equality and
    // would like for instances to be immutable wrt this equality
    String name();
}
