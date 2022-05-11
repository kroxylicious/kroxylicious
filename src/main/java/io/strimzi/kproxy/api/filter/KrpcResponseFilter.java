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

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.strimzi.kproxy.codec.DecodedResponseFrame;

/**
 * Base interface for {@code *ResponseFilter}s.
 */
public interface KrpcResponseFilter extends KrpcFilter {

    public static KrpcFilterState applyResponseFilter(
                                                      KrpcResponseFilter filter,
                                                      DecodedResponseFrame<?> decodedFrame,
                                                      KrpcFilterContext filterContext) {
        KrpcFilterState state;
        switch (decodedFrame.apiKey()) {
            case METADATA:
                state = ((MetadataResponseFilter) filter).onMetadataResponse((MetadataResponseData) decodedFrame.body(), filterContext);
                break;
            case PRODUCE:
                state = ((ProduceResponseFilter) filter).onProduceResponse((ProduceResponseData) decodedFrame.body(), filterContext);
                break;
            case API_VERSIONS:
                state = ((ApiVersionsResponseFilter) filter).onApiVersionsResponse((ApiVersionsResponseData) decodedFrame.body(), filterContext);
                break;
            case FIND_COORDINATOR:
                state = ((FindCoordinatorResponseFilter) filter).onFindCoordinatorResponse((FindCoordinatorResponseData) decodedFrame.body(), filterContext);
                break;
            case LIST_OFFSETS:
                state = ((ListOffsetsResponseFilter) filter).onListOffsetsResponse((ListOffsetsResponseData) decodedFrame.body(), filterContext);
                break;
            default:
                throw new IllegalStateException("Unsupported RPC " + decodedFrame.apiKey());
            // TODO and so on (generate this code)
        }
        return state;
    }

    public default boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
        switch (apiKey) {
            case PRODUCE:
                return this instanceof ProduceResponseFilter;
            case METADATA:
                return this instanceof MetadataResponseFilter;
            case API_VERSIONS:
                return this instanceof ApiVersionsResponseFilter;
            case FETCH:
                return this instanceof FetchResponseFilter;
            case FIND_COORDINATOR:
                return this instanceof FindCoordinatorResponseFilter;
            case LIST_OFFSETS:
                return this instanceof ListOffsetsResponseFilter;
            // TODO and so on
            default:
                throw new IllegalStateException("Unsupported API key " + apiKey);
        }
    }
}
