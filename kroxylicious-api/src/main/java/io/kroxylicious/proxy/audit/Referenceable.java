/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.net.InetSocketAddress;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;

public interface Referenceable<S extends Referenceable<S> & Loggable> {
    interface Scope<T> {}
    Scope<InetSocketAddress> ADDRESS = new Scope<InetSocketAddress>() {};
    Scope<String> VIRTUAL_CLUSTER = new Scope<String>() {};
    Scope<Integer> NODE_ID = new Scope<Integer>() {};
    Scope<String> TOPIC_NAME = new Scope<String>() {};
    Scope<String> TOPIC_ID = new Scope<String>() {};
    Scope<String> GROUP_ID = new Scope<String>() {};
    Scope<String> TRANSACTIONAL_ID = new Scope<String>() {};

    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    <T> S addCoordinate(Scope<T> scope, T identifier);
}
