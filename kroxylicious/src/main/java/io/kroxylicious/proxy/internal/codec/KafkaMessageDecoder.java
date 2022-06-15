/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.List;

import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.frame.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Abstraction for request and response decoders.
 */
public abstract class KafkaMessageDecoder extends ByteToMessageDecoder {

    protected abstract Logger log();

    public KafkaMessageDecoder() {
    }

    @Override
    protected synchronized void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.readableBytes() > 4) {
            try {
                int sof = in.readerIndex();
                int frameSize = in.readInt();
                int readable = in.readableBytes();
                if (log().isTraceEnabled()) { // avoid boxing
                    log().trace("{}: Frame of {} bytes ({} readable)", ctx, frameSize, readable);
                }
                // TODO handle too-large frames
                if (readable >= frameSize) { // We can read the whole frame
                    var idx = in.readerIndex();
                    out.add(decodeHeaderAndBody(ctx,
                            in.readSlice(frameSize), // Prevent decodeHeaderAndBody() from reading beyond the frame
                            frameSize));
                    log().trace("{}: readable: {}, having read {}", ctx, in.readableBytes(), in.readerIndex() - idx);
                    if (in.readerIndex() - idx != frameSize) {
                        throw new RuntimeException("decodeHeaderAndBody did not read all of the buffer " + in);
                    }
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

    protected abstract Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length);

}
