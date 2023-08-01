/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import io.kroxylicious.proxy.frame.Frame;

/**
 * Abstraction for request and response encoders.
 */
public abstract class KafkaMessageEncoder<F extends Frame> extends MessageToByteEncoder<F> {

    /*
     * TODO In org.apache.kafka.common.protocol.SendBuilder.buildSend Kafka gets to optimize how it writes to the
     * output buffer because it can sometimes use zero copy and so avoid needing to allocate a buffer for the whole message
     * To do similar we'd need to override io.netty.handler.codec.MessageToByteEncoder.write()
     * so we had control over buffer allocation
     */

    protected abstract Logger log();

    /**
     * This has been overridden in order to use {@link Frame#estimateEncodedSize()} to pre-size correctly the holding buffer
     * and save expensive enlarging to happen under the hood, during the encoding process.
     */
    @Override
    protected ByteBuf allocateBuffer(final ChannelHandlerContext ctx, final F msg, final boolean preferDirect) throws Exception {
        final int bytes = msg.estimateEncodedSize();
        if (preferDirect) {
            return ctx.alloc().ioBuffer(bytes);
        }
        return ctx.alloc().heapBuffer(bytes);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, F frame, ByteBuf out) throws Exception {
        log().trace("{}: Encoding {} to buffer {}", ctx, frame, out);
        frame.encode(new ByteBufAccessorImpl(out));
    }
}
