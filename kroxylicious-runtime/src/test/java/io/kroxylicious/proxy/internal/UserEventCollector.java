/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A ChannelInboundHandlerAdapter which allows asserting which users events were fired
 * both other prior handlers in a netty pipeline.
 */
public class UserEventCollector extends ChannelInboundHandlerAdapter {
    List<Object> events = new ArrayList<>();

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        Objects.requireNonNull(evt);
        events.add(evt);
        super.userEventTriggered(ctx, evt);
    }

    public Object readUserEvent() {
        return events.isEmpty() ? null : events.remove(0);
    }

    public List<Object> userEvents() {
        return List.of(events);
    }

    public void clearUserEvents() {
        events.clear();
    }
}
