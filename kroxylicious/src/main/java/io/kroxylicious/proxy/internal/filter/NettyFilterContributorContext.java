/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.netty.channel.Channel;

import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.FilterExecutors;

public record NettyFilterContributorContext(Channel channel) implements FilterContributorContext {
    @Override
    public FilterExecutors executors() {
        return new NettyFilterExecutors(channel);
    }
}
