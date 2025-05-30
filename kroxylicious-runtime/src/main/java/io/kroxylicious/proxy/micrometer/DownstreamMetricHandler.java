/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.util.Metrics;

public class DownstreamMetricHandler extends ChannelDuplexHandler {
    private final EndpointBinding binding;


    private final MeterProvider<Counter> clientToProxyCounterProvider;
    private final Integer nodeId;

    public DownstreamMetricHandler(EndpointBinding binding) {
        this.binding = binding;
        String clusterName = binding.endpointGateway().virtualCluster().getClusterName();
        this.nodeId = binding instanceof BrokerEndpointBinding brokerEndpointBinding ? brokerEndpointBinding.nodeId() : null;
        this.clientToProxyCounterProvider = Metrics.KROXYLICIOUS_CLIENT_TO_PROXY_REQUEST_TOTAL_METER_PROVIDER.apply(clusterName);

        // init - move me
        clientToProxyCounterProvider.withTags(Metrics.DECODED_LABEL, Boolean.toString(false), Metrics.API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name(), Metrics.API_VERSION_LABEL, Short.toString(
                ApiMessageType.CREATE_TOPICS.highestSupportedVersion(false)), Metrics.NODE_ID_LABEL, nodeId == null ? "bootstrap" : nodeId.toString());

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);
    }


}
