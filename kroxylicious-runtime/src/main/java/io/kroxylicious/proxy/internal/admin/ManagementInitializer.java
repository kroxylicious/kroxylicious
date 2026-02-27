/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin;

import java.util.Objects;
import java.util.Optional;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.internal.MeterRegistries;

public class ManagementInitializer extends ChannelInitializer<SocketChannel> {

    private static final String LIVEZ = "/livez";
    private static final int MAX_CONTENT_LENGTH = 10 * 1024 * 1024; // 10MB
    private final MeterRegistries registries;
    private final ManagementConfiguration managementConfiguration;

    public ManagementInitializer(MeterRegistries registries,
                                 ManagementConfiguration managementConfiguration) {
        Objects.requireNonNull(registries);
        Objects.requireNonNull(managementConfiguration);
        this.registries = registries;
        this.managementConfiguration = managementConfiguration;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
        p.addLast(new HttpServerExpectContinueHandler());
        RoutingHttpServer.RoutingHttpServerBuilder builder = RoutingHttpServer.builder();

        // Register GET endpoints
        builder.withGetRoute(LIVEZ, httpRequest -> RoutingHttpServer.responseWithStatus(httpRequest, HttpResponseStatus.OK));
        Optional.ofNullable(managementConfiguration.endpoints())
                .map(EndpointsConfiguration::maybePrometheus)
                .ifPresent(prometheusMetricsConfig -> builder.withGetRoute(PrometheusMetricsEndpoint.PATH, new PrometheusMetricsEndpoint(registries)));

        p.addLast(builder.build());
    }

}
