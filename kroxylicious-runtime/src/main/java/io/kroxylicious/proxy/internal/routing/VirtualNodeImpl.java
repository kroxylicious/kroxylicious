/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.kroxylicious.proxy.router.VirtualNode;

/**
 * Runtime implementation of {@link VirtualNode} for the port-per-broker
 * networking model. Wraps the encoded virtual node ID integer.
 */
record VirtualNodeImpl(int encodedId) implements VirtualNode {
}
