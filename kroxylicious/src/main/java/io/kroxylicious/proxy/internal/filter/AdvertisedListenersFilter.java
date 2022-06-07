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
package io.kroxylicious.proxy.internal.filter;

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.KrpcFilterState;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;

public class AdvertisedListenersFilter implements MetadataResponseFilter {

    private static final Logger LOGGER = LogManager.getLogger(AdvertisedListenersFilter.class);

    public interface AddressMapping {
        String host(String host, int port);

        int port(String host, int port);
    }

    private final AddressMapping mapping;

    public AdvertisedListenersFilter(AddressMapping mapping) {
        this.mapping = mapping;
    }

    private void mapBrokers(KrpcFilterContext context, MetadataResponseData resp) {
        for (var broker : resp.brokers()) {
            String host = mapping.host(broker.host(), broker.port());
            int port = mapping.port(broker.host(), broker.port());
            LOGGER.trace("{}: Rewriting metadata response {}:{} -> {}:{}", context, broker.host(), broker.port(), host, port);
            broker.setHost(host);
            broker.setPort(port);
        }
    }

    @Override
    public KrpcFilterState onMetadataResponse(MetadataResponseData data, KrpcFilterContext context) {
        mapBrokers(context, data);
        return KrpcFilterState.FORWARD;
    }
}
