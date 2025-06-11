/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.Optional;

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.util.Metrics;

/**
 * Responsible for emitting message metrics.
 */
public class MessageMetrics extends ChannelDuplexHandler {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<MeterProvider<Counter>> readCounterProvider;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<MeterProvider<Counter>> writeCounterProvider;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<MeterProvider<DistributionSummary>> writeSizeDistSummaryProvider;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<MeterProvider<DistributionSummary>> readSizeDistSummaryProvider;

    public MessageMetrics(MeterProvider<Counter> readCounterProvider,
                          MeterProvider<Counter> writeCounterProvider,
                          MeterProvider<DistributionSummary> readSizeDistSummaryProvider,
                          MeterProvider<DistributionSummary> writeSizeDistSummaryProvider) {
        this.readCounterProvider = Optional.ofNullable(readCounterProvider);
        this.readSizeDistSummaryProvider = Optional.ofNullable(readSizeDistSummaryProvider);
        this.writeCounterProvider = Optional.ofNullable(writeCounterProvider);
        this.writeSizeDistSummaryProvider = Optional.ofNullable(writeSizeDistSummaryProvider);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            readCounterProvider.ifPresent(provider -> count(msg, provider));
            readSizeDistSummaryProvider.ifPresent(provider -> incrementSize(msg, provider));
        }
        finally {
            super.channelRead(ctx, msg);
        }

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            writeCounterProvider.ifPresent(provider -> count(msg, provider));
            writeSizeDistSummaryProvider.ifPresent(provider -> incrementSize(msg, provider));
        }
        finally {
            super.write(ctx, msg, promise);
        }
    }

    private void count(Object msg, MeterProvider<Counter> counterProvider) {
        if (msg instanceof Frame frame) {
            var apiKey = ApiKeys.forId(frame.apiKeyId());
            short version = frame.apiVersion();
            boolean decoded = frame.isDecoded();
            counterProvider
                    .withTags(Metrics.DECODED_LABEL, Boolean.toString(decoded),
                            Metrics.API_KEY_LABEL, apiKey.name(),
                            Metrics.API_VERSION_LABEL, Short.toString(version))
                    .increment();

        }
    }

    private void incrementSize(Object msg, MeterProvider<DistributionSummary> distributionProvider) {
        if (msg instanceof Frame frame) {
            var apiKey = ApiKeys.forId(frame.apiKeyId());
            short version = frame.apiVersion();
            boolean decoded = frame.isDecoded();

            final int size;
            // TODO fix me!
            if (frame instanceof DecodedRequestFrame<?> decodedFrame) {
                size = new DecodedRequestFrame<>(decodedFrame.apiVersion(), decodedFrame.correlationId(), decodedFrame.decodeResponse(),
                        decodedFrame.header(), decodedFrame.body(), -1).estimateEncodedSize();
            }
            else if (frame instanceof DecodedResponseFrame<?> decodedFrame) {
                size = new DecodedResponseFrame<>(decodedFrame.apiVersion(), decodedFrame.correlationId(), decodedFrame.header(), decodedFrame.body(),
                        -1 /* KW FIX ME */)
                        .estimateEncodedSize();
            }
            else {
                size = frame.estimateEncodedSize();
            }
            distributionProvider
                    .withTags(Metrics.DECODED_LABEL, Boolean.toString(decoded),
                            Metrics.API_KEY_LABEL, apiKey.name(),
                            Metrics.API_VERSION_LABEL, Short.toString(version))
                    .record(size);

        }
    }

}
