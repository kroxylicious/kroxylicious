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
package io.strimzi.kproxy.internal.interceptor;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.strimzi.kproxy.api.filter.ApiVersionsResponseFilter;
import io.strimzi.kproxy.api.filter.KrpcFilterContext;
import io.strimzi.kproxy.api.filter.KrpcFilterState;

/**
 * Changes an API_VERSIONS response so that a client sees the intersection of supported version ranges for each
 * API key. This is an intrinsic part of correctly acting as a proxy.
 */
public class ApiVersionsInterceptor implements ApiVersionsResponseFilter {
    private static final Logger LOGGER = LogManager.getLogger(ApiVersionsInterceptor.class);

    private static void intersectApiVersions(String channel, ApiVersionsResponseData resp) {
        for (var key : resp.apiKeys()) {
            short apiId = key.apiKey();
            if (ApiKeys.hasId(apiId)) {
                ApiKeys apiKey = ApiKeys.forId(apiId);
                intersectApiVersion(channel, key, apiKey);
            }
        }
    }

    /**
     * Update the given {@code key}'s max and min versions so that the client uses APIs versions mutually
     * understood by both the proxy and the broker.
     * @param channel The channel.
     * @param key The key data from an upstream API_VERSIONS response.
     * @param apiKey The proxy's API key for this API.
     */
    private static void intersectApiVersion(String channel, ApiVersionsResponseData.ApiVersion key, ApiKeys apiKey) {
        short mutualMin = (short) Math.max(
                key.minVersion(),
                apiKey.messageType.lowestSupportedVersion());
        if (mutualMin != key.minVersion()) {
            LOGGER.trace("{}: {} min version changed to {} (was: {})", channel, apiKey, mutualMin, key.maxVersion());
            key.setMinVersion(mutualMin);
        }
        else {
            LOGGER.trace("{}: {} min version unchanged (is: {})", channel, apiKey, mutualMin);
        }

        short mutualMax = (short) Math.min(
                key.maxVersion(),
                apiKey.messageType.highestSupportedVersion());
        if (mutualMax != key.maxVersion()) {
            LOGGER.trace("{}: {} max version changed to {} (was: {})", channel, apiKey, mutualMin, key.maxVersion());
            key.setMaxVersion(mutualMax);
        }
        else {
            LOGGER.trace("{}: {} max version unchanged (is: {})", channel, apiKey, mutualMin);
        }
    }

    @Override
    public KrpcFilterState onApiVersionsResponse(ApiVersionsResponseData data, KrpcFilterContext context) {
        intersectApiVersions(context.channelDescriptor(), data);
        return KrpcFilterState.FORWARD;
    }
}
