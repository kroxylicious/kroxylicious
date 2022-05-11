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
package io.strimzi.kproxy.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.strimzi.kproxy.api.filter.KrpcFilter;
import io.strimzi.kproxy.api.filter.KrpcRequestFilter;
import io.strimzi.kproxy.api.filter.KrpcResponseFilter;
import io.strimzi.kproxy.codec.Correlation;
import io.strimzi.kproxy.codec.KafkaRequestDecoder;
import io.strimzi.kproxy.codec.KafkaResponseEncoder;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyInitializer.class);

    private final String remoteHost;
    private final int remotePort;
    private final FilterChainFactory filterChainFactory;
    private final boolean logNetwork;
    private final boolean logFrames;

    public KafkaProxyInitializer(String remoteHost,
                                 int remotePort,
                                 FilterChainFactory filterChainFactory,
                                 boolean logNetwork,
                                 boolean logFrames) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.filterChainFactory = filterChainFactory;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
    }

    List<KrpcRequestFilter> createRequestFilters(List<KrpcFilter> filters) {
        List<KrpcRequestFilter> requestFilters = new ArrayList<>(filters.size());
        for (var filter : filters) {
            if (filter instanceof KrpcRequestFilter) {
                requestFilters.add((KrpcRequestFilter) filter);
            }
            else if (!(filter instanceof KrpcResponseFilter)) {
                throw new RuntimeException();
            }
        }
        return requestFilters;
    }

    List<KrpcResponseFilter> createResponseFilters(List<KrpcFilter> filters) {
        List<KrpcResponseFilter> requestFilters = new ArrayList<>(filters.size());
        for (var filter : filters) {
            if (filter instanceof KrpcResponseFilter) {
                requestFilters.add((KrpcResponseFilter) filter);
            }
            else if (!(filter instanceof KrpcRequestFilter)) {
                throw new RuntimeException();
            }
        }
        return requestFilters;
    }

    /**
     * Builds a request pipeline for incoming requests from the downstream client.
     * @param filters The filters in the pipeline (response filters won't be added to the result).
     * @return A list of channel handlers
     */
    List<ChannelHandler> buildRequestPipeline(List<KrpcRequestFilter> filters) {
        // Note: we could equally use a single ChannelInboundHandler which itself dispatched to each filter.
        // Using a ChannelInboundHandler-per-filter model means that we're not occupying the CPU for the
        // whole filterchain execution => higher latency, but higher throughput.
        List<ChannelHandler> requestFilterHandlers = new ArrayList<>(filters.size());
        for (var requestFilter : filters) {
            requestFilterHandlers.add(new SingleRequestFilterHandler(requestFilter));
        }
        return requestFilterHandlers;
    }

    /**
     * Builds a response pipeline for incomping responses from the upstream server.
     * @param filters The filters in the pipeline (request filters won't be added to the result).
     * @return A list of channel handlers
     */
    List<ChannelHandler> buildResponsePipeline(List<KrpcResponseFilter> filters) {
        // Note: we could equally use a single ChannelInboundHandler which itself dispatched to each filter.
        // Using a ChannelInboundHandler-per-filter model means that we're not occupying the CPU for the
        // whole filterchain execution => higher latency, but higher throughput.
        List<ChannelHandler> responseFilterHandlers = new ArrayList<>(filters.size());
        for (var responseFilter : filters) {
            responseFilterHandlers.add(new SingleResponseFilterHandler(responseFilter));
        }
        return responseFilterHandlers;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        // TODO TLS

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        var correlation = new HashMap<Integer, Correlation>();

        ChannelPipeline pipeline = ch.pipeline();
        if (logNetwork) {
            pipeline.addLast("networkLogger", new LoggingHandler("frontend-network", LogLevel.INFO));
        }
        var filters = filterChainFactory.createFilters();
        var requestFilters = createRequestFilters(filters);
        var responseFilters = createResponseFilters(filters);
        // The decoder, this only cares about the filters
        // because it needs to know whether to decode requests
        KafkaRequestDecoder decoder = new KafkaRequestDecoder(
                requestFilters,
                responseFilters,
                correlation);
        pipeline.addLast("requestDecoder", decoder);

        var requestFilterHandlers = buildRequestPipeline(requestFilters);

        for (var handler : requestFilterHandlers) {
            ch.pipeline().addLast(handler);
        }

        pipeline.addLast("responseEncoder", new KafkaResponseEncoder());
        if (logFrames) {
            pipeline.addLast("frameLogger", new LoggingHandler("frontend-application", LogLevel.INFO));
        }

        var responseFilterHandlers = buildResponsePipeline(responseFilters);

        pipeline.addLast("frontendHandler", new KafkaProxyFrontendHandler(remoteHost,
                remotePort,
                correlation,
                responseFilterHandlers,
                logNetwork,
                logFrames));
    }

}
