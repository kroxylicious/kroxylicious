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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.ProxyConfig;
import io.kroxylicious.proxy.filter.DescribeClusterResponseFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;

/**
 * A filter that rewrites broker addresses in all relevant responses to the corresponding proxy address.
 */
public class BrokerAddressFilter implements MetadataResponseFilter, FindCoordinatorResponseFilter, DescribeClusterResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAddressFilter.class);

    public static class BrokerAddressFilterConfig extends FilterConfig {
    }

    public interface AddressMapping {
        String downstreamHost(String upstreamHost, int upstreamPort);

        int downstreamPort(String upstreamHost, int upstreamPort);
    }

    private static class FixedAddressMapping implements AddressMapping {

        private final String targetHost;
        private final int targetPort;

        public FixedAddressMapping(String targetHost, int targetPort) {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
        }

        @Override
        public String downstreamHost(String host, int port) {
            return targetHost;
        }

        @Override
        public int downstreamPort(String host, int port) {
            return targetPort;
        }
    }

    private final AddressMapping mapping;

    public BrokerAddressFilter(ProxyConfig config) {
        String proxyAddress = config.address();
        String[] proxyAddressParts = proxyAddress.split(":");

        this.mapping = new FixedAddressMapping(proxyAddressParts[0], Integer.valueOf(proxyAddressParts[1]));
    }

    @Override
    public void onMetadataResponse(MetadataResponseData data, KrpcFilterContext context) {
        for (MetadataResponseBroker broker : data.brokers()) {
            apply(context, broker, MetadataResponseBroker::host, MetadataResponseBroker::port, MetadataResponseBroker::setHost, MetadataResponseBroker::setPort);
        }
        context.forwardResponse(data);
    }

    @Override
    public void onDescribeClusterResponse(DescribeClusterResponseData data, KrpcFilterContext context) {
        for (DescribeClusterBroker broker : data.brokers()) {
            apply(context, broker, DescribeClusterBroker::host, DescribeClusterBroker::port, DescribeClusterBroker::setHost, DescribeClusterBroker::setPort);
        }
        context.forwardResponse(data);
    }

    @Override
    public void onFindCoordinatorResponse(FindCoordinatorResponseData data, KrpcFilterContext context) {
        for (Coordinator coordinator : data.coordinators()) {
            apply(context, coordinator, Coordinator::host, Coordinator::port, Coordinator::setHost, Coordinator::setPort);
        }
        context.forwardResponse(data);
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
