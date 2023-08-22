/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.Channel;

import io.kroxylicious.proxy.filter.FilterExecutors;

public record NettyFilterExecutors(Channel channel) implements FilterExecutors {
    @Override
    public ScheduledExecutorService filterExecutor() {
        return channel.eventLoop();
    }
}
