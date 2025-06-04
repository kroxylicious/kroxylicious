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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Tag;
import io.netty.bootstrap.ServerBootstrap;
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
    private final @Nullable ManagementConfiguration managementConfiguration;
    private final @NonNull List<MicrometerDefinition> micrometerConfig;
    private final @NonNull List<VirtualClusterModel> virtualClusterModels;
    private final AtomicBoolean running = new AtomicBoolean();
    private final CompletableFuture<Void> shutdown = new CompletableFuture<>();
    private final NetworkBindingOperationProcessor bindingOperationProcessor = new DefaultNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    private final @NonNull PluginFactoryRegistry pfr;
    private @Nullable MeterRegistries meterRegistries;
    private @Nullable FilterChainFactory filterChainFactory;
    private @Nullable EventGroupConfig managementEventGroup;
    private @Nullable EventGroupConfig serverEventGroup;

    public KafkaProxy(@NonNull PluginFactoryRegistry pfr, @NonNull Configuration config, @NonNull Features features) {
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

            var portConflictDefector = new PortConflictDetector();
            var managementHostPort = Optional.ofNullable(managementConfiguration)
                    .map(c -> new HostPort(c.getEffectiveBindAddress(), c.getEffectivePort()));
            portConflictDefector.validate(virtualClusterModels, managementHostPort);

            var availableCores = Runtime.getRuntime().availableProcessors();
            meterRegistries = new MeterRegistries(pfr, micrometerConfig);

            this.managementEventGroup = buildNettyEventGroups("management", availableCores, config.isUseIoUring());
            this.serverEventGroup = buildNettyEventGroups("server", availableCores, config.isUseIoUring());

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

            initDeprecatedMessageMetrics();
            STARTUP_SHUTDOWN_LOGGER.info("Kroxylicious is started");
            return this;
        }
        catch (RuntimeException e) {
            shutdown();
            throw e;
        }
    }

    @SuppressWarnings("removal")
    private void initDeprecatedMessageMetrics() {
        // Pre-register counters/summaries to avoid creating them on first request and thus skewing the request latency
        virtualClusterModels.forEach(virtualClusterModel -> {
            List<Tag> tags = Metrics.tags(Metrics.FLOWING_TAG, Metrics.DOWNSTREAM, Metrics.VIRTUAL_CLUSTER_TAG, virtualClusterModel.getClusterName());
            Metrics.taggedCounter(Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES, tags);
            Metrics.taggedCounter(Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES, tags);
        });
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

    @NonNull
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