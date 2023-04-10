/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.common.message.ResponseHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.DescribeClusterResponseFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;

/**
 * A filter that rewrites broker addresses in all relevant responses to the corresponding proxy address.
 */
public class BrokerAddressFilter implements MetadataResponseFilter, FindCoordinatorResponseFilter, DescribeClusterResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAddressFilter.class);

    private final ClusterEndpointProvider endpointProvider;

    public BrokerAddressFilter(ClusterEndpointProvider endpointProvider) {
        this.endpointProvider = endpointProvider;
    }

    @Override
    public void onMetadataResponse(ResponseHeaderData header, MetadataResponseData data, KrpcFilterContext context) {
        for (MetadataResponseBroker broker : data.brokers()) {
            apply(context, broker, MetadataResponseBroker::nodeId, MetadataResponseBroker::host, MetadataResponseBroker::port, MetadataResponseBroker::setHost,
                    MetadataResponseBroker::setPort);
        }
        context.forwardResponse(data);
    }

    @Override
    public void onDescribeClusterResponse(ResponseHeaderData header, DescribeClusterResponseData data, KrpcFilterContext context) {
        for (DescribeClusterBroker broker : data.brokers()) {
            apply(context, broker, DescribeClusterBroker::brokerId, DescribeClusterBroker::host, DescribeClusterBroker::port, DescribeClusterBroker::setHost,
                    DescribeClusterBroker::setPort);
        }
        context.forwardResponse(data);
    }

    @Override
    public void onFindCoordinatorResponse(ResponseHeaderData header, FindCoordinatorResponseData data, KrpcFilterContext context) {
        for (Coordinator coordinator : data.coordinators()) {
            apply(context, coordinator, Coordinator::nodeId, Coordinator::host, Coordinator::port, Coordinator::setHost, Coordinator::setPort);
        }
        context.forwardResponse(data);
    }

    private <T> void apply(KrpcFilterContext context, T broker, Function<T, Integer> nodeIdGetter, Function<T, String> hostGetter, ToIntFunction<T> portGetter,
                           BiConsumer<T, String> hostSetter,
                           ObjIntConsumer<T> portSetter) {
        String incomingHost = hostGetter.apply(broker);
        int incomingPort = portGetter.applyAsInt(broker);

        var downstreamAddress = endpointProvider.getBrokerAddress(nodeIdGetter.apply(broker));
        var parts = downstreamAddress.split(":");
        var host = parts[0];
        var port = Integer.parseInt(parts[1]);

        LOGGER.trace("{}: Rewriting broker address in response {}:{} -> {}:{}", context, incomingHost, incomingPort, host, port);
        hostSetter.accept(broker, host);
        portSetter.accept(broker, port);
    }
}
