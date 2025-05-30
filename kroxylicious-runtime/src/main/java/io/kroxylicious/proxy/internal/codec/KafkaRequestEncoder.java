/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.InternalRequestFrame;
import io.kroxylicious.proxy.internal.util.Metrics;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaRequestEncoder extends KafkaMessageEncoder<RequestFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestEncoder.class);

    public static final int LENGTH = 4;
    public static final int API_KEY = 2;
    public static final int API_VERSION = 2;
    private final CorrelationManager correlationManager;
    private final String clusterName;
    private final Integer nodeId;
    private final MeterProvider<Counter> proxyToClientCounterProvider;

    public KafkaRequestEncoder(CorrelationManager correlationManager, String clusterName, @Nullable Integer nodeId) {
        this.correlationManager = correlationManager;
        this.clusterName = clusterName;
        this.nodeId = nodeId;

        this.proxyToClientCounterProvider = Metrics.KROXYLICIOUS_PROXY_TO_SERVER_REQUEST_TOTAL_METER_PROVIDER.apply(clusterName);
        // init - move me
        proxyToClientCounterProvider.withTags(Metrics.DECODED_LABEL, Boolean.toString(false), Metrics.API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name(), Metrics.API_VERSION_LABEL, Short.toString(
                ApiMessageType.CREATE_TOPICS.highestSupportedVersion(false)), Metrics.NODE_ID_LABEL, nodeId == null ? "bootstrap" : nodeId.toString());

    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RequestFrame frame, ByteBuf out) throws Exception {
        super.encode(ctx, frame, out);
        // TODO re-reading from the encoded buffer like this is ugly
        // probably better to just include apiKey and apiVersion in the frame
        var ri = out.readerIndex();
        var wi = out.writerIndex();
        out.readerIndex(LENGTH);
        short apiKey = out.readShort();
        short apiVersion = out.readShort();
        boolean hasResponse = frame.hasResponse();
        boolean decodeResponse = frame.decodeResponse();
        proxyToClientCounterProvider.withTags(Metrics.DECODED_LABEL, Boolean.toString(!(frame instanceof OpaqueRequestFrame)),
                Metrics.API_KEY_LABEL, ApiKeys.forId(apiKey).name(),
                Metrics.API_VERSION_LABEL, Short.toString(apiVersion),
                Metrics.NODE_ID_LABEL, nodeId == null ? "bootstrap" : nodeId.toString()).increment();

        int downstreamCorrelationId = frame.correlationId();
        int upstreamCorrelationId = correlationManager.putBrokerRequest(apiKey,
                apiVersion,
                downstreamCorrelationId,
                hasResponse,
                frame instanceof InternalRequestFrame ? ((InternalRequestFrame<?>) frame).recipient() : null,
                frame instanceof InternalRequestFrame ? ((InternalRequestFrame<?>) frame).promise() : null, decodeResponse);
        out.writerIndex(LENGTH + API_KEY + API_VERSION);
        out.writeInt(upstreamCorrelationId);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: {} downstream correlation id {} assigned upstream correlation id: {}",
                    ctx, ApiKeys.forId(apiKey), downstreamCorrelationId, upstreamCorrelationId);
        }
        out.readerIndex(ri);
        out.writerIndex(wi);
    }

}
