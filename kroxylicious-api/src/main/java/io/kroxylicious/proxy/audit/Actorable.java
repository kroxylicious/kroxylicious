/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.net.InetSocketAddress;
import java.util.List;

import io.kroxylicious.proxy.authentication.Principal;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;

public interface Actorable<S extends Actorable<S> & Loggable> {
    interface ActorKey<T> {}
    ActorKey<String> PROXY = new ActorKey<String>() {
    };
    ActorKey<InetSocketAddress> CLIENT_ADDRESS_KEY = new ActorKey<InetSocketAddress>() {
    };
    ActorKey<String> KAFKA_SESSION_ID = new ActorKey<String>() {
    };
    ActorKey<List<Principal>> CLIENT_PRINCIPLES = new ActorKey<List<Principal>>() {
    };
    ActorKey<InetSocketAddress> SERVER_ADDRESS_KEY = new ActorKey<InetSocketAddress>() {
    };
    ActorKey<Integer> SERVER_NODE_ID_KEY = new ActorKey<Integer>() {
    };

    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    <T> S addActor(ActorKey<T> key, T value);
}
