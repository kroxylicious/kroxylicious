/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static java.util.Objects.requireNonNull;

/**
 * An inbound channel handler that sits after the Filter Chain. It informs a {@link ClientConnectionStateMachine} about
 * messages read from client channel, which have implicitly completed traversal of the Filter Chain.
 * <p>
 * The intent is to have this handler installed as the last handler in the pipeline (besides a catch-all error logger)
 * to inform the {@link ClientConnectionStateMachine} about messages that have traversed (or been sent from) the Filter chain
 * and should be forwarded to the {@link KafkaProxyBackendHandler}. We can infer that the state machine should be in
 * {@link ClientConnectionState.Forwarding} or {@link ClientConnectionState.Closed} state, because messages should not flow through
 * {@link KafkaProxyFrontendHandler} and arrive at this handler until the state is {@link ClientConnectionState.Forwarding}.
 * </p>
 */
class FilterChainCompletionHandler extends ChannelInboundHandlerAdapter {
    private final ClientConnectionStateMachine clientConnectionStateMachine;

    FilterChainCompletionHandler(ClientConnectionStateMachine clientConnectionStateMachine) {
        this.clientConnectionStateMachine = requireNonNull(clientConnectionStateMachine);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        clientConnectionStateMachine.onClientFilterChainComplete(msg);
    }
}
