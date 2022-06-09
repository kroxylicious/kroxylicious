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

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.ToIntFunction;

import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
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
        String downstreamHost(String upstreamHost, int upstreamPort);

        int downstreamPort(String upstreamHost, int upstreamPort);
    }

    private final AddressMapping mapping;

    public BrokerAddressFilter(AddressMapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public KrpcFilterState onMetadataResponse(MetadataResponseData data, KrpcFilterContext context) {
        for (MetadataResponseBroker broker : data.brokers()) {
            apply(context, broker, MetadataResponseBroker::host, MetadataResponseBroker::port, MetadataResponseBroker::setHost, MetadataResponseBroker::setPort);
        }
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onDescribeClusterResponse(DescribeClusterResponseData data, KrpcFilterContext context) {
        for (DescribeClusterBroker broker : data.brokers()) {
            apply(context, broker, DescribeClusterBroker::host, DescribeClusterBroker::port, DescribeClusterBroker::setHost, DescribeClusterBroker::setPort);
        }
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onFindCoordinatorResponse(FindCoordinatorResponseData data, KrpcFilterContext context) {
        for (Coordinator coordinator : data.coordinators()) {
            apply(context, coordinator, Coordinator::host, Coordinator::port, Coordinator::setHost, Coordinator::setPort);
        }
        return KrpcFilterState.FORWARD;
    }

    private <T> void apply(KrpcFilterContext context, T broker, Function<T, String> hostGetter, ToIntFunction<T> portGetter, BiConsumer<T, String> hostSetter,
                           ObjIntConsumer<T> portSetter) {
        String incomingHost = hostGetter.apply(broker);
        int incomingPort = portGetter.applyAsInt(broker);

        String host = mapping.downstreamHost(incomingHost, incomingPort);
        int port = mapping.downstreamPort(incomingHost, incomingPort);

        LOGGER.trace("{}: Rewriting broker address in response {}:{} -> {}:{}", context, incomingHost, incomingPort, host, port);
        hostSetter.accept(broker, host);
        portSetter.accept(broker, port);
    }
}
