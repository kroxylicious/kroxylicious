/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.PortConflictDetector;
import io.kroxylicious.proxy.internal.admin.AdminHttpInitializer;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.internal.net.DefaultNetworkBindingOperationProcessor;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.internal.net.NetworkBindingOperationProcessor;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Objects.requireNonNull;

public final class KafkaProxy implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxy.class);
    private static final Logger STARTUP_SHUTDOWN_LOGGER = LoggerFactory.getLogger("io.kroxylicious.proxy.StartupShutdownLogger");

    private record EventGroupConfig(String name, EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz) {

        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }
    }

    private final @NonNull Configuration config;
    private final @Nullable AdminHttpConfiguration adminHttpConfig;
    private final @NonNull List<MicrometerDefinition> micrometerConfig;
    private final @NonNull List<VirtualCluster> virtualClusters;
    private final AtomicBoolean running = new AtomicBoolean();
    private final CompletableFuture<Void> shutdown = new CompletableFuture<>();
    private final NetworkBindingOperationProcessor bindingOperationProcessor = new DefaultNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    private final @NonNull PluginFactoryRegistry pfr;
    private @Nullable MeterRegistries meterRegistries;
    private @Nullable FilterChainFactory filterChainFactory;
    private @Nullable EventGroupConfig adminEventGroup;
    private @Nullable EventGroupConfig serverEventGroup;
    private @Nullable Channel metricsChannel;

    public KafkaProxy(@NonNull PluginFactoryRegistry pfr, @NonNull Configuration config, @NonNull Features features) {
        this.pfr = requireNonNull(pfr);
        this.config = validate(requireNonNull(config), requireNonNull(features));
        this.virtualClusters = config.virtualClusterModel();
        this.adminHttpConfig = config.adminHttpConfig();
        this.micrometerConfig = config.getMicrometer();
    }

    @VisibleForTesting
    static Configuration validate(Configuration config, Features features) {
        List<String> errorMessages = features.supports(config);
        if (!errorMessages.isEmpty()) {
            String message = "invalid configuration: " + String.join(",", errorMessages);
            LOGGER.error(message);
            throw new IllegalConfigurationException(message);
        }
        return config;
    }

    /**
     * Starts this proxy.
     * @return This proxy.
     */
    public KafkaProxy startup() throws InterruptedException {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("This proxy is already running");
        }
        try {
            STARTUP_SHUTDOWN_LOGGER.info("Kroxylicious is starting");

            var portConflictDefector = new PortConflictDetector();
            Optional<HostPort> adminHttpHostPort = Optional.ofNullable(shouldBindAdminEndpoint() ? new HostPort(adminHttpConfig.host(), adminHttpConfig.port()) : null);
            portConflictDefector.validate(virtualClusters, adminHttpHostPort);

            var availableCores = Runtime.getRuntime().availableProcessors();
            meterRegistries = new MeterRegistries(micrometerConfig);

            this.adminEventGroup = buildNettyEventGroups("admin", availableCores, config.isUseIoUring());
            this.serverEventGroup = buildNettyEventGroups("server", availableCores, config.isUseIoUring());

            maybeStartMetricsListener(adminEventGroup, meterRegistries);

            var overrideMap = getApiKeyMaxVersionOverride(config);
            ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(overrideMap);
            this.filterChainFactory = new FilterChainFactory(pfr, config.filters());
            var tlsServerBootstrap = buildServerBootstrap(serverEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, true, endpointRegistry, endpointRegistry, false, Map.of(), apiVersionsService));
            var plainServerBootstrap = buildServerBootstrap(serverEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, false, endpointRegistry, endpointRegistry, false, Map.of(), apiVersionsService));

            bindingOperationProcessor.start(plainServerBootstrap, tlsServerBootstrap);

            // TODO: startup/shutdown should return a completionstage
            CompletableFuture.allOf(
                    virtualClusters.stream().map(vc -> endpointRegistry.registerVirtualCluster(vc).toCompletableFuture()).toArray(CompletableFuture[]::new))
                    .join();

            // Pre-register counters/summaries to avoid creating them on first request and thus skewing the request latency
            // TODO add a virtual host tag to metrics
            Metrics.inboundDownstreamMessagesCounter();
            Metrics.inboundDownstreamDecodedMessagesCounter();
            return this;
        }
        catch (RuntimeException | InterruptedException e) {
            shutdown();
            throw e;
        }
    }

    private Map<ApiKeys, Short> getApiKeyMaxVersionOverride(Configuration config) {
        Map<String, Number> apiKeyIdMaxVersion = config.development()
                .map(m -> m.get("apiKeyIdMaxVersionOverride"))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .orElse(Map.of());

        return apiKeyIdMaxVersion.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> ApiKeys.valueOf(e.getKey()),
                        e -> e.getValue().shortValue()));
    }

    private ServerBootstrap buildServerBootstrap(EventGroupConfig virtualHostEventGroup, KafkaProxyInitializer kafkaProxyInitializer) {
        return new ServerBootstrap().group(virtualHostEventGroup.bossGroup(), virtualHostEventGroup.workerGroup())
                .channel(virtualHostEventGroup.clazz())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(kafkaProxyInitializer)
                .childOption(ChannelOption.TCP_NODELAY, true);
    }

    private EventGroupConfig buildNettyEventGroups(String name, int availableCores, boolean useIoUring) {
        final Class<? extends ServerChannel> channelClass;
        final EventLoopGroup bossGroup;
        final EventLoopGroup workerGroup;

        if (useIoUring) {
            if (!IOUring.isAvailable()) {
                throw new IllegalStateException("io_uring not available due to: " + IOUring.unavailabilityCause());
            }
            bossGroup = new IOUringEventLoopGroup(1);
            workerGroup = new IOUringEventLoopGroup(availableCores);
            channelClass = IOUringServerSocketChannel.class;
        }
        else if (Epoll.isAvailable()) {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup(availableCores);
            channelClass = EpollServerSocketChannel.class;
        }
        else if (KQueue.isAvailable()) {
            bossGroup = new KQueueEventLoopGroup(1);
            workerGroup = new KQueueEventLoopGroup(availableCores);
            channelClass = KQueueServerSocketChannel.class;
        }
        else {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup(availableCores);
            channelClass = NioServerSocketChannel.class;
        }
        return new EventGroupConfig(name, bossGroup, workerGroup, channelClass);
    }

    private void maybeStartMetricsListener(EventGroupConfig eventGroupConfig,
                                           MeterRegistries meterRegistries)
            throws InterruptedException {
        if (shouldBindAdminEndpoint()) {
            ServerBootstrap metricsBootstrap = new ServerBootstrap().group(eventGroupConfig.bossGroup(), eventGroupConfig.workerGroup())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .channel(eventGroupConfig.clazz())
                    .childHandler(new AdminHttpInitializer(meterRegistries, adminHttpConfig));
            LOGGER.info("Binding metrics endpoint: {}:{}", adminHttpConfig.host(), adminHttpConfig.port());
            metricsChannel = metricsBootstrap.bind(adminHttpConfig.host(), adminHttpConfig.port()).sync().channel();
        }
    }

    private boolean shouldBindAdminEndpoint() {
        return adminHttpConfig != null
                && adminHttpConfig.endpoints().maybePrometheus().isPresent();
    }

    /**
     * Blocks while this proxy is running.
     * This should only be called after a successful call to {@link #startup()}.
     * @throws InterruptedException
     */
    public void block() throws Exception {
        if (!running.get()) {
            throw new IllegalStateException("This proxy is not running");
        }
        shutdown.join();
    }

    /**
     * Shuts down a running proxy.
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        if (!running.getAndSet(false)) {
            throw new IllegalStateException("This proxy is not running");
        }
        try {
            STARTUP_SHUTDOWN_LOGGER.info("Shutting down");
            endpointRegistry.shutdown().handle((u, t) -> {
                bindingOperationProcessor.close();
                var closeFutures = new ArrayList<Future<?>>();
                if (serverEventGroup != null) {
                    closeFutures.addAll(serverEventGroup.shutdownGracefully());
                }
                if (adminEventGroup != null) {
                    closeFutures.addAll(adminEventGroup.shutdownGracefully());
                }
                closeFutures.forEach(Future::syncUninterruptibly);
                if (filterChainFactory != null) {
                    filterChainFactory.close();
                }
                if (t != null) {
                    if (t instanceof RuntimeException re) {
                        throw re;
                    }
                    else {
                        throw new RuntimeException(t);
                    }
                }
                return null;
            }).toCompletableFuture().join();
            if (meterRegistries != null) {
                meterRegistries.close();
            }
        }
        finally {
            adminEventGroup = null;
            serverEventGroup = null;
            metricsChannel = null;
            meterRegistries = null;
            filterChainFactory = null;
            shutdown.complete(null);
            LOGGER.info("Shut down completed.");

        }
    }

    @Override
    public void close() throws Exception {
        if (running.get()) {
            shutdown();
        }
    }

}
