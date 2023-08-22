/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import static org.assertj.core.api.Assertions.assertThat;

class NettyFilterContributorContextTest {

    @Test
    void testExecutor() {
        EmbeddedChannel channel = new EmbeddedChannel();
        NettyFilterContributorContext context = new NettyFilterContributorContext(channel);
        assertThat(context.executors().filterExecutor()).isSameAs(channel.eventLoop());
    }

}