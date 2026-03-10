/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.net.SocketAddress;

import io.kroxylicious.proxy.audit.ServerActor;

import edu.umd.cs.findbugs.annotations.Nullable;

public record ServerActorImpl(SocketAddress tgtAddr, String hostname, @Nullable Integer nodeId) implements ServerActor {

}
