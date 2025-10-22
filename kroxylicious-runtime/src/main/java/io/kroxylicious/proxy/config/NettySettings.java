/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record NettySettings(Optional<Integer> workerThreadCount, Optional<NettyTransport> nettyTransport) {

    @JsonIgnore
    public int activeWorkerThreadCount() {
        return workerThreadCount.orElse(Runtime.getRuntime().availableProcessors());
    }
}
