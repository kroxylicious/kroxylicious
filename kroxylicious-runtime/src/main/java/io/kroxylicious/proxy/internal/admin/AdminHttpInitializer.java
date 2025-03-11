/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.internal.MeterRegistries;

public class AdminHttpInitializer extends ChannelInitializer<SocketChannel> {

    private static final String LIVEZ = "/livez";
    private final MeterRegistries registries;
    private final AdminHttpConfiguration adminHttpConfiguration;

    public AdminHttpInitializer(MeterRegistries registries, AdminHttpConfiguration adminHttpConfiguration) {
        this.registries = registries;
        this.adminHttpConfiguration = adminHttpConfiguration;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpServerExpectContinueHandler());
        RoutingHttpServer.RoutingHttpServerBuilder builder = RoutingHttpServer.builder();
        builder.withRoute(LIVEZ, httpRequest -> RoutingHttpServer.responseWithStatus(httpRequest, HttpResponseStatus.OK));
        adminHttpConfiguration.endpoints().maybePrometheus().ifPresent(prometheusMetricsConfig -> {
            builder.withRoute(PrometheusMetricsEndpoint.PATH, new PrometheusMetricsEndpoint(registries));
        });
        p.addLast(builder.build());
    }

}
