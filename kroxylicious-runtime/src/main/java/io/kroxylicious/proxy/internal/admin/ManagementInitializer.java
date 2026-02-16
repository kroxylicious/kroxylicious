/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.admin.ConfigReloadEndpointConfig;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.admin.endpoints.ConfigurationReloadEndpoint;
import io.kroxylicious.proxy.internal.admin.format.JsonResponseFormatter;
import io.kroxylicious.proxy.internal.admin.reload.ConfigurationReloadOrchestrator;
import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestProcessor;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ManagementInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagementInitializer.class);
    private static final String LIVEZ = "/livez";
    private static final int MAX_CONTENT_LENGTH = 10 * 1024 * 1024; // 10MB
    private final MeterRegistries registries;
    private final ManagementConfiguration managementConfiguration;
    private final @Nullable ConfigurationReloadOrchestrator reloadOrchestrator;

    public ManagementInitializer(MeterRegistries registries,
                                 ManagementConfiguration managementConfiguration,
                                 @Nullable ConfigurationReloadOrchestrator reloadOrchestrator) {
        Objects.requireNonNull(registries);
        Objects.requireNonNull(managementConfiguration);
        this.registries = registries;
        this.managementConfiguration = managementConfiguration;
        this.reloadOrchestrator = reloadOrchestrator;
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

        // Register POST endpoints
        Optional.ofNullable(managementConfiguration.endpoints())
                .map(EndpointsConfiguration::configReload)
                .filter(ConfigReloadEndpointConfig::enabled)
                .ifPresent(reloadConfig -> registerReloadEndpoint(builder, reloadConfig));

        p.addLast(builder.build());
    }

    /**
     * Register the configuration reload endpoint if enabled.
     */
    private void registerReloadEndpoint(RoutingHttpServer.RoutingHttpServerBuilder builder,
                                       ConfigReloadEndpointConfig config) {
        if (reloadOrchestrator == null) {
            LOGGER.warn("Configuration reload endpoint is enabled but orchestrator not available - skipping endpoint registration");
            return;
        }

        // Create request processor
        ConfigParser parser = new ConfigParser();
        long timeoutSeconds = config.getTimeout().toSeconds();
        ReloadRequestProcessor processor = new ReloadRequestProcessor(
                parser,
                reloadOrchestrator,
                timeoutSeconds);

        // Create endpoint
        ConfigurationReloadEndpoint endpoint = new ConfigurationReloadEndpoint(
                processor,
                new JsonResponseFormatter());

        builder.withPostRoute(ConfigurationReloadEndpoint.PATH, endpoint);

        LOGGER.warn("⚠️  Configuration reload endpoint enabled at {} (INSECURE - no authentication)",
                ConfigurationReloadEndpoint.PATH);
    }

}
