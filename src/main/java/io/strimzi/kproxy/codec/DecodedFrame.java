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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;

/**
 * A frame that has been decoded (as opposed to an {@link OpaqueFrame}).
 * @param <H>
 */
public abstract class DecodedFrame<H extends ApiMessage> implements Frame {
    private static final Logger LOGGER = LogManager.getLogger(DecodedFrame.class);

    protected final H header;
    protected final ApiMessage body;
    protected final short apiVersion;

    public DecodedFrame(short apiVersion, H header, ApiMessage body) {
        this.header = header;
        this.apiVersion = apiVersion;
        this.body = body;
    }

    protected abstract short headerVersion();

    public H header() {
        return header;
    }

    public ApiMessage body() {
        return body;
    }

    public ApiKeys apiKey() {
        return ApiKeys.forId(body.apiKey());
    }

    public short apiVersion() {
        return apiVersion;
    }

    @Override
    public final void encode(ByteBuf out) {
        var headerVersion = headerVersion();
        MessageSizeAccumulator sizer = new MessageSizeAccumulator();
        ObjectSerializationCache cache = new ObjectSerializationCache();
        header().addSize(sizer, cache, headerVersion);
        body().addSize(sizer, cache, apiVersion());
        ByteBufAccessor writable = new ByteBufAccessor(out);
        int length = sizer.totalSize();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Writing {} with 4 byte length ({}) plus bytes of header {}, and body {} to {}",
                    getClass().getSimpleName(), length, header, body, out);
        }
        writable.writeInt(length);
        header.write(writable, cache, headerVersion);
        body.write(writable, cache, apiVersion());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                ApiKeys.forId(apiVersion) + "(" + apiVersion + ")v" + apiVersion +
                ", header=" + header +
                ", body=" + body +
                ')';
    }
}
