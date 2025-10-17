/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.file.Path;
import java.time.Duration;
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
import io.kroxylicious.proxy.internal.ConfigWatcherService;
import io.kroxylicious.proxy.internal.ConfigurationChangeContext;
import io.kroxylicious.proxy.internal.ConfigurationChangeHandler;
import io.kroxylicious.proxy.internal.ConnectionDrainManager;
import io.kroxylicious.proxy.internal.ConnectionTracker;
import io.kroxylicious.proxy.internal.FilterChangeDetector;
import io.kroxylicious.proxy.internal.InFlightMessageTracker;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.PortConflictDetector;
import io.kroxylicious.proxy.internal.VirtualClusterChangeDetector;
import io.kroxylicious.proxy.internal.VirtualClusterManager;
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

    private record EventGroupConfig(String name, EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz) {

        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }
    }

    private Configuration config;
    private Features features;
    private final @Nullable ManagementConfiguration managementConfiguration;
    private final List<MicrometerDefinition> micrometerConfig;
    private List<VirtualClusterModel> virtualClusterModels;
    private final AtomicBoolean running = new AtomicBoolean();
    private final CompletableFuture<Void> shutdown = new CompletableFuture<>();
    private final NetworkBindingOperationProcessor bindingOperationProcessor = new DefaultNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    private final PluginFactoryRegistry pfr;
    private final ConnectionTracker connectionTracker = new ConnectionTracker();
    private final InFlightMessageTracker inFlightTracker = new InFlightMessageTracker();
    private final ConnectionDrainManager connectionDrainManager;
    private final VirtualClusterManager virtualClusterManager;
    private final ConfigurationChangeHandler configurationChangeHandler;
    private ConfigWatcherService configWatcherService;
    private final Path configFilePath;
    private @Nullable MeterRegistries meterRegistries;
    private @Nullable FilterChainFactory filterChainFactory;
    private @Nullable EventGroupConfig managementEventGroup;
    private @Nullable EventGroupConfig serverEventGroup;

    public KafkaProxy(PluginFactoryRegistry pfr, Configuration config, Features features) {
        this(pfr, config, features, null);
    }

    public KafkaProxy(PluginFactoryRegistry pfr, Configuration config, Features features, Path configFilePath) {
        this.pfr = requireNonNull(pfr);
        this.config = validate(requireNonNull(config), requireNonNull(features));
        this.features = features;
        this.virtualClusterModels = config.virtualClusterModel(pfr);
        this.managementConfiguration = config.management();
        this.micrometerConfig = config.getMicrometer();
        this.configFilePath = configFilePath;

        // Initialize connection management components
        this.connectionDrainManager = new ConnectionDrainManager(connectionTracker, inFlightTracker);
        this.virtualClusterManager = new VirtualClusterManager(endpointRegistry, connectionDrainManager);

        // Initialize configuration change handler with direct list of detectors
        this.configurationChangeHandler = new ConfigurationChangeHandler(
                List.of(
                        new VirtualClusterChangeDetector(),
                        new FilterChangeDetector()),
                virtualClusterManager);
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
            meterRegistries = new MeterRegistries(pfr, micrometerConfig);
            initVersionInfoMetric();

            var portConflictDefector = new PortConflictDetector();
            var managementHostPort = Optional.ofNullable(managementConfiguration)
                    .map(c -> new HostPort(c.getEffectiveBindAddress(), c.getEffectivePort()));
            portConflictDefector.validate(virtualClusterModels, managementHostPort);

            var availableCores = Runtime.getRuntime().availableProcessors();

            this.managementEventGroup = buildNettyEventGroups("management", availableCores, config.isUseIoUring());
            this.serverEventGroup = buildNettyEventGroups("server", availableCores, config.isUseIoUring());

            var managementFuture = maybeStartManagementListener(managementEventGroup, meterRegistries);

            var overrideMap = getApiKeyMaxVersionOverride(config);
            ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(overrideMap);
            this.filterChainFactory = new FilterChainFactory(pfr, config.filterDefinitions());

            var tlsServerBootstrap = buildServerBootstrap(serverEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, true, endpointRegistry, endpointRegistry, false, Map.of(),
                            apiVersionsService, connectionTracker, connectionDrainManager, inFlightTracker));
            var plainServerBootstrap = buildServerBootstrap(serverEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, false, endpointRegistry, endpointRegistry, false, Map.of(),
                            apiVersionsService, connectionTracker, connectionDrainManager, inFlightTracker));

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

            // Start configuration file watcher if config file path is provided
            if (configFilePath != null) {
                startConfigurationWatcher(configFilePath)
                        .thenRun(() -> LOGGER.info("Configuration file watcher started successfully for: {}", configFilePath))
                        .exceptionally(e -> {
                            LOGGER.error("Failed to start configuration watcher for: {}", configFilePath, e);
                            return null;
                        });
            }
            else {
                LOGGER.info("No configuration file path provided - hot-reload disabled");
            }

            STARTUP_SHUTDOWN_LOGGER.info("Kroxylicious is started");
            return this;
        }
        catch (RuntimeException e) {
            shutdown();
            throw e;
        }
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
     * Starts watching the configuration file for changes and enables hot-reloading.
     *
     * @param configFilePath the path to the configuration file to watch
     * @return CompletableFuture that completes when the watcher is started
     */
    public CompletableFuture<Void> startConfigurationWatcher(Path configFilePath) {
        if (configWatcherService != null) {
            LOGGER.warn("Configuration watcher is already running");
            return CompletableFuture.completedFuture(null);
        }

        LOGGER.info("Starting configuration file watcher for: {}", configFilePath);
        this.configWatcherService = new ConfigWatcherService(
                configFilePath,
                this::handleConfigurationChange,
                Duration.ofMillis(500) // 500ms debounce delay
        );

        return configWatcherService.start();
    }

    /**
     * Stops the configuration file watcher.
     *
     * @return CompletableFuture that completes when the watcher is stopped
     */
    public CompletableFuture<Void> stopConfigurationWatcher() {
        if (configWatcherService == null) {
            return CompletableFuture.completedFuture(null);
        }

        LOGGER.info("Stopping configuration file watcher");
        return configWatcherService.stop().thenRun(() -> {
            configWatcherService = null;
        });
    }

    /**
     * Handles configuration changes detected by the file watcher.
     * Delegates to ConfigurationChangeHandler for processing.
     *
     * @param newConfig the new configuration
     */
    private void handleConfigurationChange(Configuration newConfig) {
        try {
            Configuration newValidatedConfig = validate(newConfig, features);
            Configuration oldConfig = this.config;

            // Create models once to avoid excessive logging during change detection
            List<VirtualClusterModel> oldModels = oldConfig.virtualClusterModel(pfr);
            List<VirtualClusterModel> newModels = newValidatedConfig.virtualClusterModel(pfr);
            ConfigurationChangeContext changeContext = new ConfigurationChangeContext(
                    oldConfig, newValidatedConfig, oldModels, newModels);

            // Delegate to the configuration change handler
            configurationChangeHandler.handleConfigurationChange(changeContext)
                    .thenRun(() -> {
                        // Update the stored configuration after successful hot-reload
                        this.config = newValidatedConfig;
                        // Synchronize the virtualClusterModels with the new configuration to ensure consistency
                        this.virtualClusterModels = newModels;
                        LOGGER.info("Configuration and virtual cluster models successfully updated");
                    });
        }
        catch (Exception e) {
            LOGGER.error("Failed to validate or process configuration change", e);
        }
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

            // Stop configuration watcher first
            if (configWatcherService != null) {
                try {
                    stopConfigurationWatcher().join();
                }
                catch (Exception e) {
                    LOGGER.warn("Error stopping configuration watcher during shutdown", e);
                }
            }

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

            // Close connection management components
            connectionDrainManager.close();
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