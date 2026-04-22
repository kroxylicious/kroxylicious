/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class KafkaProxyGatewayHandler extends ChannelInboundHandlerAdapter {

    private final ProxyChannelStateMachine stateMachine;

    public KafkaProxyGatewayHandler(ProxyChannelStateMachine stateMachine) {
        Objects.requireNonNull(stateMachine, "state machine was null");
        this.stateMachine = stateMachine;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        stateMachine.onClientActive(this);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        stateMachine.onClientInactive();
        super.channelInactive(ctx);
    }
}
