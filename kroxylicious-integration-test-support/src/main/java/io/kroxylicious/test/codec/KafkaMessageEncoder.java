/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Abstraction for request and response encoders.
 * @param <F> type of frame to encode
 */
public abstract class KafkaMessageEncoder<F extends Frame> extends MessageToByteEncoder<F> {

    /**
     * Create KafkaMessageEncoder
     */
    protected KafkaMessageEncoder() {
    }

    /**
     * Logger
     * @return logger
     */
    protected abstract Logger log();

    /**
     * This has been overridden in order to use {@link Frame#estimateEncodedSize()} to pre-size correctly the holding buffer
     * and save expensive enlarging to happen under the hood, during the encoding process.
     */
    @Override
    protected ByteBuf allocateBuffer(final ChannelHandlerContext ctx, final F msg, final boolean preferDirect) {
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
