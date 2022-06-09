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

import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.filter.DescribeClusterResponseFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.KrpcFilterState;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;

/**
 * A filter that rewrites broker addresses in all relevant responses to the corresponding proxy address.
 */
public class BrokerAddressFilter implements MetadataResponseFilter, FindCoordinatorResponseFilter, DescribeClusterResponseFilter {

    private static final Logger LOGGER = LogManager.getLogger(BrokerAddressFilter.class);

    public interface AddressMapping {
        String host(String host, int port);

        int port(String host, int port);
    }

    private final AddressMapping mapping;

    public BrokerAddressFilter(AddressMapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public KrpcFilterState onMetadataResponse(MetadataResponseData data, KrpcFilterContext context) {
        mapBrokers(context, data);
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onDescribeClusterResponse(DescribeClusterResponseData data, KrpcFilterContext context) {
        mapBrokers(context, data);
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onFindCoordinatorResponse(FindCoordinatorResponseData data, KrpcFilterContext context) {
        mapCoordinators(context, data);
        return KrpcFilterState.FORWARD;
    }

    private void mapBrokers(KrpcFilterContext context, MetadataResponseData data) {
        for (var broker : data.brokers()) {
            String host = mapping.host(broker.host(), broker.port());
            int port = mapping.port(broker.host(), broker.port());
            LOGGER.trace("{}: Rewriting metadata response {}:{} -> {}:{}", context, broker.host(), broker.port(), host, port);
            broker.setHost(host);
            broker.setPort(port);
        }
    }

    private void mapBrokers(KrpcFilterContext context, DescribeClusterResponseData data) {
        for (var broker : data.brokers()) {
            String host = mapping.host(broker.host(), broker.port());
            int port = mapping.port(broker.host(), broker.port());
            LOGGER.trace("{}: Rewriting describe cluster response {}:{} -> {}:{}", context, broker.host(), broker.port(), host, port);
            broker.setHost(host);
            broker.setPort(port);
        }
    }

    private void mapCoordinators(KrpcFilterContext context, FindCoordinatorResponseData data) {
        for (Coordinator coordinator : data.coordinators()) {
            String host = mapping.host(coordinator.host(), coordinator.port());
            int port = mapping.port(coordinator.host(), coordinator.port());
            LOGGER.trace("{}: Rewriting find coordinator response {}:{} -> {}:{}", context, coordinator.host(), coordinator.port(), host, port);
            coordinator.setHost(host);
            coordinator.setPort(port);
        }
    }
}
