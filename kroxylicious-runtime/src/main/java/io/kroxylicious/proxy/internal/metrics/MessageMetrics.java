/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.Optional;

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.util.Metrics;

/**
 * Responsible for emitting message metrics.
 */
public class MessageMetrics extends ChannelDuplexHandler {
    private final MeterProvider<Counter> readCounterProvider;
    private final MeterProvider<Counter> writeCounterProvider;

    public MessageMetrics(MeterProvider<Counter> readCounterProvider, MeterProvider<Counter> writeCounterProvider) {
        this.readCounterProvider = readCounterProvider;
        this.writeCounterProvider = writeCounterProvider;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            Optional.ofNullable(readCounterProvider)
                    .ifPresent(counter -> count(msg, counter));
        }
        finally {
            super.channelRead(ctx, msg);
        }

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            Optional.ofNullable(writeCounterProvider)
                    .ifPresent(counter -> count(msg, counter));
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

}
