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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;

/**
 * A frame in the Kafka protocol which has not been decoded.
 * The wrapped buffer <strong>does not</strong> include the frame size prefix.
 */
public abstract class OpaqueFrame implements Frame {
    private static final Logger LOGGER = LogManager.getLogger(OpaqueFrame.class);
    private final int length;
    private final ByteBuf buf;

    public OpaqueFrame(ByteBuf buf, int length) {
        this.length = length;
        this.buf = buf.asReadOnly();
    }

    @Override
    public void encode(ByteBuf out) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Writing {} with 4 byte length ({}) plus {} bytes from buffer {} to {}",
                    getClass().getSimpleName(), length, buf.readableBytes(), buf, out);
        }
        out.writeInt(length);
        out.writeBytes(buf, length);
        buf.release();
    }

    /* test */ ByteBuf buf() {
        return buf;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "length=" + length +
                ", buf=" + buf +
                ')';
    }
}
