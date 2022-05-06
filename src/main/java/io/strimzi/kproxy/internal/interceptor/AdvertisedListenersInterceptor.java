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

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.strimzi.kproxy.api.filter.FilterContext;
import io.strimzi.kproxy.api.filter.MetadataResponseFilter;
import io.strimzi.kproxy.interceptor.Interceptor;
import io.strimzi.kproxy.interceptor.RequestHandler;
import io.strimzi.kproxy.interceptor.ResponseHandler;

public class AdvertisedListenersInterceptor implements Interceptor, MetadataResponseFilter {

    private static final Logger LOGGER = LogManager.getLogger(AdvertisedListenersInterceptor.class);

    public interface AddressMapping {
        String host(String host, int port);

        int port(String host, int port);
    }

    private final AddressMapping mapping;

    public AdvertisedListenersInterceptor(AddressMapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public boolean shouldDecodeRequest(ApiKeys apiKey, int apiVersion) {
        return false;
    }

    @Override
    public boolean shouldDecodeResponse(ApiKeys apiKey, int apiVersion) {
        return apiKey == ApiKeys.METADATA;
    }

    @Override
    public RequestHandler requestHandler() {
        return null;
    }

    @Override
    public ResponseHandler responseHandler() {
        return (responseFrame, channel) -> {
            var resp = (MetadataResponseData) responseFrame.body();
            mapBrokers(channel, resp);

            return responseFrame;
        };
    }

    private void mapBrokers(FilterContext context, MetadataResponseData resp) {
        for (var broker : resp.brokers()) {
            String host = mapping.host(broker.host(), broker.port());
            int port = mapping.port(broker.host(), broker.port());
            LOGGER.trace("{}: Rewriting metadata response {}:{} -> {}:{}", context, broker.host(), broker.port(), host, port);
            broker.setHost(host);
            broker.setPort(port);
        }
    }

    @Override
    public MetadataResponseData onMetadataResponse(MetadataResponseData data, FilterContext context) {
        mapBrokers(context, data);
        return null;
    }
}
