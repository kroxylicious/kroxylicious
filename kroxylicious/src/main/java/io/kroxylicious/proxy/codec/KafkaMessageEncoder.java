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
package io.kroxylicious.proxy.codec;

import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

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
        frame.encode(out);
    }
}
