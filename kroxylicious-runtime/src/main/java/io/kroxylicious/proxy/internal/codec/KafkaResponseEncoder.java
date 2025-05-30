/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Meter.MeterProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.ResponseFrame;

public class KafkaResponseEncoder extends KafkaMessageEncoder<ResponseFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseEncoder.class);
    private final String clusterName;
    private final Integer nodeId;
    //private final MeterProvider proxyToClientCounterProvider; // WRONG!

    public KafkaResponseEncoder(String clusterName, Integer nodeId) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
//        this.proxyToClientCounterProvider = Metrics.KROXYLICIOUS_PROXY_TO_SERVER_REQUEST_TOTAL_METER_PROVIDER.apply(clusterName);
//        // init - move me
//        proxyToClientCounterProvider.withTags(Metrics.DECODED_LABEL, Boolean.toString(false), Metrics.API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name(), Metrics.API_VERSION_LABEL, Short.toString(
//                ApiMessageType.CREATE_TOPICS.highestSupportedVersion(false)), Metrics.NODE_ID_LABEL, nodeId == null ? "bootstrap" : nodeId.toString());
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ResponseFrame frame, ByteBuf out) throws Exception {
//        // decode value based on frame type
//        proxyToClientCounterProvider.withTags(Metrics.DECODED_LABEL, Boolean.toString(false), Metrics.API_KEY_LABEL, frame.)

        super.encode(ctx, frame, out);
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

}
