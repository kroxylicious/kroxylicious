/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFutureListener;
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
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.NetworkDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.PortConflictDetector;
import io.kroxylicious.proxy.internal.admin.ManagementInitializer;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.internal.net.DefaultNetworkBindingOperationProcessor;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.internal.net.NetworkBindingOperationProcessor;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Objects.requireNonNull;

public final class KafkaProxy implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxy.class);
    private static final Logger STARTUP_SHUTDOWN_LOGGER = LoggerFactory.getLogger("io.kroxylicious.proxy.StartupShutdownLogger");

    @VisibleForTesting
    record EventGroupConfig(String name, EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz) {

        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }

        public static EventGroupConfig build(String name, int availableCores, boolean useIoUring) {
            if (useIoUring) {
                if (!IOUring.isAvailable()) {
                    throw new IllegalStateException("io_uring not available due to: " + IOUring.unavailabilityCause());
                }
                return new EventGroupConfig(name, new IOUringEventLoopGroup(1), new IOUringEventLoopGroup(availableCores), IOUringServerSocketChannel.class);
            }
            if (Epoll.isAvailable()) {
                return new EventGroupConfig(name, new EpollEventLoopGroup(1), new EpollEventLoopGroup(availableCores), EpollServerSocketChannel.class);
            }
            if (KQueue.isAvailable()) {
                return new EventGroupConfig(name, new KQueueEventLoopGroup(1), new KQueueEventLoopGroup(availableCores), KQueueServerSocketChannel.class);
            }
            return new EventGroupConfig(name, new NioEventLoopGroup(1), new NioEventLoopGroup(availableCores), NioServerSocketChannel.class);
        }
    }

    private final Configuration config;
    private final @Nullable ManagementConfiguration managementConfiguration;
    private final List<MicrometerDefinition> micrometerConfig;
    private final List<VirtualClusterModel> virtualClusterModels;
    private final AtomicBoolean running = new AtomicBoolean();
    private final CompletableFuture<Void> shutdown = new CompletableFuture<>();
    private final NetworkBindingOperationProcessor bindingOperationProcessor = new DefaultNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    private final PluginFactoryRegistry pfr;
    private @Nullable MeterRegistries meterRegistries;
    private @Nullable FilterChainFactory filterChainFactory;
    private @Nullable EventGroupConfig managementEventGroup;
    private @Nullable EventGroupConfig serverEventGroup;

    public KafkaProxy(PluginFactoryRegistry pfr, Configuration config, Features features) {
        this.pfr = requireNonNull(pfr);
        this.config = validate(requireNonNull(config), requireNonNull(features));
        this.virtualClusterModels = config.virtualClusterModel(pfr);
        this.managementConfiguration = config.management();
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

    @VisibleForTesting
    @Nullable
    EventGroupConfig managementEventGroup() {
        return managementEventGroup;
    }

    @VisibleForTesting
    @Nullable
    EventGroupConfig serverEventGroup() {
        return serverEventGroup;
    }

    /**
     * Starts this proxy.
     * @return This proxy.
     */
    @SuppressWarnings("java:S5738")
    public KafkaProxy startup() {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("This proxy is already running");
        }
        try {
            STARTUP_SHUTDOWN_LOGGER.info("Kroxylicious is starting");
            meterRegistries = new MeterRegistries(pfr, micrometerConfig);
            initVersionInfoMetric();

            var portConflictDefector = new PortConflictDetector();
            var managementHostPort = Optional.ofNullable(managementConfiguration)
                    .map(c -> new HostPort(c.getEffectiveBindAddress(), c.getEffectivePort()));
            portConflictDefector.validate(virtualClusterModels, managementHostPort);

            int proxyWorkerThreadCount = resolveThreadCount(NetworkDefinition::proxy);
            int managementWorkerThreadCount = resolveThreadCount(NetworkDefinition::management);

            this.managementEventGroup = EventGroupConfig.build("management", managementWorkerThreadCount, config.isUseIoUring());
            this.serverEventGroup = EventGroupConfig.build("proxy", proxyWorkerThreadCount, config.isUseIoUring());

            enableNettyMetrics(managementEventGroup, serverEventGroup);

            var managementFuture = maybeStartManagementListener(managementEventGroup, meterRegistries);

            var overrideMap = getApiKeyMaxVersionOverride(config);
            ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(overrideMap);
            this.filterChainFactory = new FilterChainFactory(pfr, config.filterDefinitions());

            var tlsServerBootstrap = buildServerBootstrap(serverEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, true, endpointRegistry, endpointRegistry, false, Map.of(), apiVersionsService));
            var plainServerBootstrap = buildServerBootstrap(serverEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, false, endpointRegistry, endpointRegistry, false, Map.of(), apiVersionsService));

            bindingOperationProcessor.start(plainServerBootstrap, tlsServerBootstrap);

            // TODO: startup/shutdown should return a completionstage
            CompletableFuture.allOf(
                    Stream.concat(Stream.of(managementFuture),
                            virtualClusterModels.stream()
                                    .flatMap(vc -> vc.gateways().values().stream())
                                    .map(vcl -> endpointRegistry.registerVirtualCluster(vcl).toCompletableFuture()))
                            .toArray(CompletableFuture[]::new))
                    .join();

            STARTUP_SHUTDOWN_LOGGER.info("Kroxylicious is started");
            return this;
        }
        catch (RuntimeException e) {
            shutdown();
            throw e;
        }
    }

    private void enableNettyMetrics(final EventGroupConfig... eventGroups) {
        Metrics.bindNettyAllocatorMetrics(ByteBufAllocator.DEFAULT);
        for (final var group : eventGroups) {
            Metrics.bindNettyEventExecutorMetrics(group.bossGroup(), group.workerGroup());
        }
    }

    private Integer resolveThreadCount(Function<NetworkDefinition, NettySettings> settingsSupplier) {
        return Optional.ofNullable(config.network())
                .map(settingsSupplier)
                .flatMap(NettySettings::workerThreadCount)
                .orElse(Runtime.getRuntime().availableProcessors());
    }

    private void initVersionInfoMetric() {
        Metrics.versionInfoMetric(VersionInfo.VERSION_INFO);
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

    private CompletableFuture<Void> maybeStartManagementListener(EventGroupConfig eventGroupConfig, MeterRegistries meterRegistries) {
        return Optional.ofNullable(managementConfiguration)
                .map(mc -> {
                    var metricsBootstrap = new ServerBootstrap().group(eventGroupConfig.bossGroup(), eventGroupConfig.workerGroup())
                            .option(ChannelOption.SO_REUSEADDR, true)
                            .channel(eventGroupConfig.clazz())
                            .childHandler(new ManagementInitializer(meterRegistries, mc));
                    LOGGER.info("Binding management endpoint: {}:{}", mc.getEffectiveBindAddress(), mc.getEffectivePort());

                    var future = new CompletableFuture<Void>();
                    metricsBootstrap.bind(managementConfiguration.getEffectiveBindAddress(), managementConfiguration.getEffectivePort())
                            .addListener((ChannelFutureListener) channelFuture -> ForkJoinPool.commonPool().execute(() -> {
                                // we complete on a separate thread so that any chained work won't get run on the Netty thread.
                                if (channelFuture.cause() != null) {
                                    future.completeExceptionally(channelFuture.cause());
                                }
                                else {
                                    future.complete(null);
                                }
                            }));
                    return future;
                }).orElseGet(() -> CompletableFuture.completedFuture(null));
    }

    /**
     * Blocks while this proxy is running.
     * This should only be called after a successful call to {@link #startup()}.
     */
    public void block() {
        if (!running.get()) {
            throw new IllegalStateException("This proxy is not running");
        }
        shutdown.join();
    }

    /**
     * Shuts down a running proxy.
     */
    public void shutdown() {
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
                if (managementEventGroup != null) {
                    closeFutures.addAll(managementEventGroup.shutdownGracefully());
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
            managementEventGroup = null;
            serverEventGroup = null;
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