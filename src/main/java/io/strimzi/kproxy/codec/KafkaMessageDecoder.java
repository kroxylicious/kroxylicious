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

import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.strimzi.kproxy.message.GenericPayload;

public class KafkaMessageDecoder extends ByteToMessageDecoder {

    Map<Integer, List<Object>> deepInspectionHandlers;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        while (in.readableBytes() > 4) {
            int size = in.readInt();
            System.err.println("Frame has size " + size);
            // TODO handle too-large frames
            if (in.readableBytes() > size + 4) {
                // TODO for new requests this will be zigzag encoded
                short apiKey = in.readShort();
                System.err.println("apiKey " + apiKey);
                // TODO for new requests this will be zigzag encoded
                short apiVersion = in.readShort();
                System.err.println("apiVersion " + apiVersion);
                // TODO Determine the header version from the key and api version (i.e. apiVersion >= firstFlexVersion ? 2 : 1)
                // TODO decode the RequestHeaderData
                // Decide if we should deep deserialize this API: Decode a generic payload or API-specific payload
                int deepInspectionKey = apiKey << 16 | apiVersion;
                List<Object> handlers = deepInspectionHandlers.getOrDefault(deepInspectionKey, List.of());
                Object message;
                if (handlers.isEmpty()) {
                    message = new GenericPayload(apiKey, apiVersion, in);
                } else {
                    // TODO deserialize according to the API key and version
                    // but for now just return buf
                    message = in.slice(in.readerIndex(), in.readerIndex() + size);
                }
                out.add(message);
            }
        }
    }
}
