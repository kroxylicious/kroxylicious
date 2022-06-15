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
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.protocol.ApiKeys;

import io.netty.buffer.ByteBuf;

public class OpaqueRequestFrame extends OpaqueFrame implements RequestFrame {

    private final boolean decodeResponse;

    /**
     * @param buf The message buffer (excluding the frame size)
     * @param correlationId
     * @param decodeResponse
     * @param length
     */
    public OpaqueRequestFrame(ByteBuf buf,
                              int correlationId,
                              boolean decodeResponse,
                              int length) {
        super(buf, correlationId, length);
        this.decodeResponse = decodeResponse;
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }

    @Override
    public String toString() {
        int index = buf.readerIndex();
        try {
            var apiId = buf.readShort();
            // TODO handle unknown api key
            ApiKeys apiKey = ApiKeys.forId(apiId);
            short apiVersion = buf.readShort();
            return getClass().getSimpleName() + "(" +
                    "length=" + length +
                    ", apiKey=" + apiKey +
                    ", apiVersion=" + apiVersion +
                    ", buf=" + buf +
                    ')';
        }
        finally {
            buf.readerIndex(index);
        }
    }
}
