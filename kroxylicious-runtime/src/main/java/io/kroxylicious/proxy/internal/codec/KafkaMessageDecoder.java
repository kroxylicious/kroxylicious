/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.List;

import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import io.kroxylicious.proxy.frame.Frame;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Abstraction for request and response decoders.
 */
abstract class KafkaMessageDecoder extends ByteToMessageDecoder {

    private final int socketFrameMaxSize;
    private final @Nullable KafkaMessageListener listener;

    protected abstract Logger log();

    KafkaMessageDecoder(int socketFrameMaxSize,
                        @Nullable KafkaMessageListener listener) {
        this.socketFrameMaxSize = socketFrameMaxSize;
        this.listener = listener;
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.readableBytes() > 4) {
            try {
                int sof = in.readerIndex();
                int frameSize = in.readInt();
                if (frameSize > socketFrameMaxSize) {
                    throw new FrameOversizedException(socketFrameMaxSize, frameSize);
                }
                int readable = in.readableBytes();
                if (log().isTraceEnabled()) { // avoid boxing
                    log().trace("{}: Frame of {} bytes ({} readable)", ctx, frameSize, readable);
                }
                // TODO handle too-large frames
                if (readable >= frameSize) { // We can read the whole frame
                    validateRead(ctx, in, out, frameSize);
                }
                else {
                    in.readerIndex(sof);
                    break;
                }
            }
            catch (Exception e) {
                log().error("{}: Error in decoder", ctx, e);
                throw e;
            }
        }
    }

    private int readSingleFrame(ChannelHandlerContext ctx, ByteBuf in, List<Object> out, int frameSize) {
        var idx = in.readerIndex();
        var frame = decodeHeaderAndBody(ctx,
                in.readSlice(frameSize), // Prevent decodeHeaderAndBody() from reading beyond the frame
                frameSize);
        out.add(frame);
        if (listener != null) {
            listener.onMessage(frame, frameSize + Frame.FRAME_SIZE_LENGTH);
        }
        return idx;
    }

    private void validateRead(ChannelHandlerContext ctx, ByteBuf in, List<Object> out, int frameSize) {
        var idx = readSingleFrame(ctx, in, out, frameSize);
        log().trace("{}: readable: {}, having read {}", ctx, in.readableBytes(), in.readerIndex() - idx);
        if (in.readerIndex() - idx != frameSize) {
            throw new IllegalStateException("decodeHeaderAndBody did not read all of the buffer " + in);
        }
    }

    protected abstract Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length);

}
