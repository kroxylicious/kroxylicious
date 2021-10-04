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
package io.strimzi.kproxy.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.logging.log4j.Logger;

/**
 * Encodes {@link KafkaFrame}s.
 */
public abstract class KafkaMessageEncoder extends MessageToByteEncoder<KafkaFrame> {

    /* TODO In org.apache.kafka.common.protocol.SendBuilder.buildSend Kafka gets to optimize how it writes to the
     * output buffer because it can sometimes use zero copy and so avoid needing to allocate a buffer for the whole message
     * To do similar we'd need to override io.netty.handler.codec.MessageToByteEncoder.write()
     * so we had control over buffer allocation
     */

    protected abstract Logger log();

    @Override
    protected void encode(ChannelHandlerContext ctx, KafkaFrame frame, ByteBuf out) throws Exception {
        log().trace("Encoding {}", frame);
        MessageSizeAccumulator sizer = new MessageSizeAccumulator();
        ObjectSerializationCache cache = new ObjectSerializationCache();
        frame.header().addSize(sizer, cache, frame.headerVersion());
        frame.body().addSize(sizer, cache, frame.apiVersion());
        ByteBufAccessor writable = new ByteBufAccessor(out);
        writable.writeInt(sizer.totalSize());
        frame.header().write(writable, cache, frame.headerVersion());
        frame.body().write(writable, cache, frame.apiVersion());
    }
}
