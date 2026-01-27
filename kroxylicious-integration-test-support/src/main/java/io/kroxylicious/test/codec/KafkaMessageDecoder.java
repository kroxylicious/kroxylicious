/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import java.util.List;

import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Abstraction for request and response decoders.
 */
public abstract class KafkaMessageDecoder extends ByteToMessageDecoder {

    /**
     * Get the logger to use
     * @return logger
     */
    protected abstract Logger log();

    /**
     * Create a KafkaMessageDecoder
     */
    protected KafkaMessageDecoder() {
    }

    @SuppressWarnings("java:S2139") // sonar doesn't understand our logging strategy
    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.readableBytes() > 4) {
            try {
                int sof = in.readerIndex();
                int frameSize = in.readInt();
                int readable = in.readableBytes();
                if (log().isTraceEnabled()) { // avoid boxing
                    log().trace("{}: Frame of {} bytes ({} readable)", ctx, frameSize, readable);
                }
                if (readable >= frameSize) { // We can read the whole frame
                    var idx = in.readerIndex();
                    ByteBuf slice = in.readSlice(frameSize);
                    out.add(decodeHeaderAndBody(ctx,
                            slice, // Prevent decodeHeaderAndBody() from reading beyond the frame
                            frameSize));
                    log().trace("{}: readable: {}, having read {}", ctx, in.readableBytes(), in.readerIndex() - idx);
                    if (slice.readableBytes() > 0) {
                        throw new KafkaCodecException("decodeHeaderAndBody did not read all of the buffer " + slice);
                    }
                }
                else {
                    in.readerIndex(sof);
                    break;
                }
            }
            catch (Exception e) {
                log().error("{}: Error in decoder", ctx, e);
                throw new KafkaCodecException("Error decoding KafkaMessage", e);
            }
        }
    }

    /**
     * Decode the header and body
     * @param ctx handler context
     * @param in buffer slice to decode from
     * @param length size of the slice
     * @return a Frame
     */
    protected abstract Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length);

}
