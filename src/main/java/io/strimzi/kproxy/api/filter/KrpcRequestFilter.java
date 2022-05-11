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
package io.strimzi.kproxy.api.filter;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.strimzi.kproxy.codec.DecodedRequestFrame;

/**
 * Base interfaces for {@code *RequestFilter}s.
 */
public interface KrpcRequestFilter extends KrpcFilter {

    public static KrpcFilterState applyRequestFilter(
                                                     KrpcRequestFilter filter,
                                                     DecodedRequestFrame<?> decodedFrame,
                                                     KrpcFilterContext filterContext) {
        KrpcFilterState state;
        switch (decodedFrame.apiKey()) {
            case METADATA:
                state = ((MetadataRequestFilter) filter).onMetadataRequest((MetadataRequestData) decodedFrame.body(), filterContext);
                break;
            case PRODUCE:
                state = ((ProduceRequestFilter) filter).onProduceRequest((ProduceRequestData) decodedFrame.body(), filterContext);
                break;
            default:
                throw new IllegalStateException("Unsupported RPC " + decodedFrame.apiKey());
            // TODO and so on (generate this code)
        }
        return state;
    }

    default boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion) {
        switch (apiKey) {
            case PRODUCE:
                return this instanceof ProduceRequestFilter;
            case METADATA:
                return this instanceof MetadataRequestFilter;
            case FETCH:
                return this instanceof FetchRequestFilter;
            case API_VERSIONS:
                return this instanceof ApiVersionsRequestFilter;
            case SASL_AUTHENTICATE:
                return this instanceof SaslAuthenticateRequestFilter;
            case SASL_HANDSHAKE:
                return this instanceof SaslHandshakeRequestFilter;
            // TODO and so on
            default:
                throw new IllegalStateException("Unsupported API key " + apiKey);
        }
    }
}
