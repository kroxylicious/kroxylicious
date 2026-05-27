/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
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
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
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
import io.kroxylicious.proxy.internal.VirtualClusterLifecycle;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.admin.ManagementInitializer;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.internal.net.DefaultNetworkBindingOperationProcessor;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.internal.net.NetworkBindingOperationProcessor;
import io.kroxylicious.proxy.internal.reload.ConfigurationReloadOrchestrator;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ReconfigureResult;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Objects.requireNonNull;

public final class KafkaProxy implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxy.class);
    private static final Logger STARTUP_SHUTDOWN_LOGGER = LoggerFactory.getLogger("io.kroxylicious.proxy.StartupShutdownLogger");

    private static final int JRE_FEATURE_VERSION = Runtime.version().feature();
    private static final TreeSet<Integer> TESTED_JRE_VERSIONS = new TreeSet<>(Set.of(21, 25));

    @VisibleForTesting
    record EventGroupConfig(String name, EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz,
                            Duration shutdownQuietPeriod, Duration shutdownTimeout) {

        @SuppressWarnings("java:S1452")
        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(shutdownQuietPeriod.toNanos(), shutdownTimeout.toNanos(), TimeUnit.NANOSECONDS),
                    workerGroup.shutdownGracefully(shutdownQuietPeriod.toNanos(), shutdownTimeout.toNanos(), TimeUnit.NANOSECONDS));
        }

        public static EventGroupConfig build(String name, Configuration configuration, Function<NetworkDefinition, NettySettings> settingsSupplier, boolean useIoUring) {
            final Class<? extends ServerChannel> channelClass;
            final EventLoopGroup bossGroup;
            final EventLoopGroup workerGroup;
            final IoHandlerFactory ioHandlerFactory;

            // Specifying 0 threads means we apply Netty defaults which are (2 * availableCores) or the system property io.netty.eventLoopThreads.
            int workerThreadCount = resolveThreadCount(configuration, settingsSupplier);
            if (useIoUring && !IoUring.isAvailable()) {
                throw new IllegalStateException("io_uring not available due to: " + IoUring.unavailabilityCause());
            }
            if (IoUring.isAvailable() && useIoUring) {
                ioHandlerFactory = IoUringIoHandler.newFactory();
                channelClass = IoUringServerSocketChannel.class;
            }
            else if (Epoll.isAvailable()) {
                ioHandlerFactory = EpollIoHandler.newFactory();
                channelClass = EpollServerSocketChannel.class;
            }
            else if (KQueue.isAvailable()) {
                ioHandlerFactory = KQueueIoHandler.newFactory();
                channelClass = KQueueServerSocketChannel.class;
            }
            else {
                ioHandlerFactory = NioIoHandler.newFactory();
                channelClass = NioServerSocketChannel.class;
            }

            bossGroup = new MultiThreadIoEventLoopGroup(workerThreadCount, ioHandlerFactory);
            workerGroup = new MultiThreadIoEventLoopGroup(workerThreadCount, ioHandlerFactory);

            var nettySettings = getNettySettings(configuration, settingsSupplier);
            var quietPeriod = resolveQuietPeriod(nettySettings);
            var timeout = nettySettings.flatMap(NettySettings::shutdownTimeout).orElse(Duration.ofSeconds(15));

            return new EventGroupConfig(name, bossGroup, workerGroup, channelClass, quietPeriod, timeout);
        }

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private static Duration resolveQuietPeriod(Optional<NettySettings> nettySettings) {
            return nettySettings.flatMap(NettySettings::shutdownQuietPeriod)
                    .orElse(Duration.ofSeconds(2));
        }

        private static int resolveThreadCount(Configuration configuration, Function<NetworkDefinition, NettySettings> settingsSupplier) {
            return getNettySettings(configuration, settingsSupplier)
                    .flatMap(NettySettings::workerThreadCount)
                    .orElse(Runtime.getRuntime().availableProcessors());
        }
    }

    private enum LifecycleState {
        NEW {
            @Override
            boolean canTransitionTo(LifecycleState target) {
                return switch (target) {
                    case STARTING, STOPPING -> true;
                    case NEW, STARTED, STOPPED -> false;
                };
            }
        },
        STARTING {
            @Override
            boolean canTransitionTo(LifecycleState target) {
                return switch (target) {
                    case STARTED, STOPPING -> true;
                    case NEW, STARTING, STOPPED -> false;
                };
            }
        },
        STARTED {
            @Override
            boolean canTransitionTo(LifecycleState target) {
                return switch (target) {
                    case STOPPING -> true;
                    case NEW, STARTING, STARTED, STOPPED -> false;
                };
            }
        },
        STOPPING {
            @Override
            boolean canTransitionTo(LifecycleState target) {
                return switch (target) {
                    case STOPPED -> true;
                    case NEW, STARTING, STARTED, STOPPING -> false;
                };
            }
        },
        STOPPED {
            @Override
            boolean canTransitionTo(LifecycleState target) {
                return switch (target) {
                    case NEW, STARTING, STARTED, STOPPING, STOPPED -> false;
                };
            }
        };

        abstract boolean canTransitionTo(LifecycleState target);
    }

    private final Configuration config;
    private final @Nullable ManagementConfiguration managementConfiguration;
    private final List<MicrometerDefinition> micrometerConfig;
    private final List<VirtualClusterModel> virtualClusterModels;
    private final AtomicReference<LifecycleState> state = new AtomicReference<>(LifecycleState.NEW);
    private final CompletableFuture<Void> shutdown = new CompletableFuture<>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            KafkaProxy.this.shutdown();
            return false;
        }
    };
    private final NetworkBindingOperationProcessor bindingOperationProcessor = new DefaultNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    private final PluginFactoryRegistry pfr;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private @Nullable MeterRegistries meterRegistries;
    private @Nullable FilterChainFactory filterChainFactory;

    private @Nullable ConfigurationReloadOrchestrator reconfigureOrchestrator;
    private @Nullable EventGroupConfig managementEventGroup;
    private @Nullable EventGroupConfig proxyEventGroup;

    public KafkaProxy(PluginFactoryRegistry pfr, Configuration config, Features features) {
        this(pfr, config, features, defaultRegistry(config, pfr));
    }

    @VisibleForTesting
    KafkaProxy(PluginFactoryRegistry pfr, Configuration config, Features features, VirtualClusterRegistry virtualClusterRegistry) {
        this.pfr = requireNonNull(pfr);
        this.config = validate(requireNonNull(config), requireNonNull(features));
        this.virtualClusterRegistry = requireNonNull(virtualClusterRegistry);
        this.virtualClusterModels = virtualClusterRegistry.virtualClusterModels();
        this.managementConfiguration = config.management();
        this.micrometerConfig = config.getMicrometer();
    }

    private static VirtualClusterRegistry defaultRegistry(Configuration config, PluginFactoryRegistry pfr) {
        var models = config.virtualClusterModel(pfr);
        return new VirtualClusterRegistry(models, (clusterName, cause) -> {
            if (cause.isPresent()) {
                STARTUP_SHUTDOWN_LOGGER.atWarn()
                        .addKeyValue("virtualCluster", clusterName)
                        .addKeyValue("error", cause.get().getMessage())
                        .log("Virtual cluster reached terminal stopped state due to failure, proxy shutdown required");
            }
            else {
                STARTUP_SHUTDOWN_LOGGER.atInfo()
                        .addKeyValue("virtualCluster", clusterName)
                        .log("Virtual cluster stopped");
            }
        });
    }

    @VisibleForTesting
    static Configuration validate(Configuration config, Features features) {
        List<String> errorMessages = features.supports(config);
        if (!errorMessages.isEmpty()) {
            String message = "invalid configuration: " + String.join(",", errorMessages);
            LOGGER.atError()
                    .addKeyValue("message", message)
                    .log("Invalid configuration");
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
    EventGroupConfig proxyEventGroup() {
        return proxyEventGroup;
    }

    @VisibleForTesting
    CompletableFuture<Void> shutdownFuture() {
        return shutdown;
    }

    /**
     * Starts this proxy.
     * @return a future that completes when the proxy stops (normally or exceptionally).
     */
    public CompletableFuture<Void> startup() {
        // The CAS is the primary guard against concurrent startup; the STOPPING/STOPPED
        // check is belt-and-braces since the real concurrency guard is in shutdown().
        return transitionTo(LifecycleState.STARTING, this::doStartup, current -> {
            if (current == LifecycleState.STOPPING || current == LifecycleState.STOPPED) {
                throw new IllegalStateException("KafkaProxy is not restartable");
            }
            return shutdown; // STARTING or STARTED — idempotent
        });
    }

    private CompletableFuture<Void> doStartup() {
        try {
            logJdkInfo();

            STARTUP_SHUTDOWN_LOGGER.atInfo()
                    .log("Kroxylicious is starting");

            meterRegistries = new MeterRegistries(pfr, micrometerConfig);
            initVersionInfoMetric();

            var portConflictDefector = new PortConflictDetector();
            var managementHostPort = Optional.ofNullable(managementConfiguration)
                    .map(c -> new HostPort(c.getEffectiveBindAddress(), c.getEffectivePort()));
            portConflictDefector.validate(virtualClusterModels, managementHostPort);

            this.managementEventGroup = EventGroupConfig.build("management", config, NetworkDefinition::management, config.isUseIoUring());
            this.proxyEventGroup = EventGroupConfig.build("proxy", config, NetworkDefinition::proxy, config.isUseIoUring());

            enableNettyMetrics(managementEventGroup, proxyEventGroup);

            var managementFuture = maybeStartManagementListener(managementEventGroup, meterRegistries);

            var overrideMap = getApiKeyMaxVersionOverride(config);
            ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(overrideMap);
            this.filterChainFactory = new FilterChainFactory(pfr, config.filterDefinitions());
            this.reconfigureOrchestrator = new ConfigurationReloadOrchestrator(
                    config, virtualClusterRegistry, endpointRegistry, pfr,
                    ConfigurationReloadOrchestrator.defaultDetectors());

            Optional<NettySettings> proxyNettySettings = getNettySettings(config, NetworkDefinition::proxy);
            var proxyProtocolMode = config.proxyProtocolMode();
            var tlsServerBootstrap = buildServerBootstrap(proxyEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, true, endpointRegistry, endpointRegistry, proxyProtocolMode,
                            apiVersionsService, proxyNettySettings, virtualClusterRegistry));
            var plainServerBootstrap = buildServerBootstrap(proxyEventGroup,
                    new KafkaProxyInitializer(filterChainFactory, pfr, false, endpointRegistry, endpointRegistry, proxyProtocolMode,
                            apiVersionsService, proxyNettySettings, virtualClusterRegistry));

            bindingOperationProcessor.start(plainServerBootstrap, tlsServerBootstrap);

            // TODO: startup/shutdown should return a completionstage
            CompletableFuture.allOf(
                    Stream.concat(Stream.of(managementFuture),
                            virtualClusterModels.stream()
                                    .flatMap(vc -> vc.gateways().values().stream())
                                    .map(vcl -> endpointRegistry.registerVirtualCluster(vcl).toCompletableFuture()))
                            .toArray(CompletableFuture[]::new))
                    .join();

            virtualClusterModels.forEach(model -> virtualClusterRegistry.initializationSucceeded(model.getClusterName()));

            STARTUP_SHUTDOWN_LOGGER.atInfo()
                    .log("Kroxylicious is started");
            return transitionTo(LifecycleState.STARTED, () -> shutdown, lifecycleState -> {
                throw new LifecycleException("failed to start Kroxylicious lifecycle state");
            });
        }
        catch (RuntimeException e) {
            STARTUP_SHUTDOWN_LOGGER.atError()
                    .setCause(e)
                    .log("Exception during startup, shutting down");
            // TODO: the onVirtualClusterStopped callback should drive the serve:none policy (triggering proxy shutdown)
            // rather than relying on the caller to call shutdown() separately. Currently the callback only logs.
            // All VCs are failed with the same exception because startup is all-or-nothing:
            // initializationSucceeded is only called after all VCs register successfully (line 263).
            var wrapped = new LifecycleException("Startup completed exceptionally", e);
            virtualClusterModels.forEach(model -> virtualClusterRegistry.initializationFailed(model.getClusterName(), e));
            shutdown.completeExceptionally(wrapped); // claim the future before doShutdown() can complete it normally
            transitionTo(LifecycleState.STOPPING, this::doShutdown, lifecycleState -> {
            });
            throw wrapped;
        }
    }

    private static void logJdkInfo() {
        if (!TESTED_JRE_VERSIONS.contains(JRE_FEATURE_VERSION)) {
            String versionStatus = "untested";
            String deprecatedMessage = "";

            if (JRE_FEATURE_VERSION < TESTED_JRE_VERSIONS.first()) {
                versionStatus = "deprecated";
                deprecatedMessage = " The ability to run Kroxylicious on JRE %s will be removed in a future release.".formatted(JRE_FEATURE_VERSION);
            }

            STARTUP_SHUTDOWN_LOGGER.atWarn()
                    .addKeyValue("versionStatus", versionStatus)
                    .addKeyValue("jreFeatureVersion", JRE_FEATURE_VERSION)
                    .addKeyValue("testedJreVersion", TESTED_JRE_VERSIONS.first())
                    .log("Detected JRE version, running Kroxylicious is only tested on LTS releases, if you find any issues, please try to re-create them on one of the tested JREs"
                            + deprecatedMessage);
        }
    }

    private void transitionTo(LifecycleState targetState,
                              Runnable onTransition,
                              Consumer<LifecycleState> onNoTransition) {
        this.transitionTo(targetState, () -> {
            onTransition.run();
            return null;
        }, lifecycleState -> {
            onNoTransition.accept(lifecycleState);
            return null;
        });
    }

    /**
     *
     * @param targetState target state
     * @param onTransition executed if this call transitions to the targetState
     * @return the last observed state, could be the targetState if successfully transitioned, else last
     */
    private <T> T transitionTo(LifecycleState targetState,
                               Supplier<T> onTransition,
                               Function<LifecycleState, T> onNoTransition) {
        boolean transitioned = false;
        while (!transitioned) {
            LifecycleState currentState = state.get();
            if (currentState == targetState) {
                // something else has already transitioned to this state
                return onNoTransition.apply(currentState);
            }
            if (!currentState.canTransitionTo(targetState)) {
                return onNoTransition.apply(currentState);
            }
            transitioned = state.compareAndSet(currentState, targetState);
        }
        return onTransition.get();
    }

    private static Optional<NettySettings> getNettySettings(Configuration configuration, Function<NetworkDefinition, NettySettings> settingsSupplier) {
        return Optional.ofNullable(configuration.network())
                .map(settingsSupplier);
    }

    private void enableNettyMetrics(final EventGroupConfig... eventGroups) {
        Metrics.bindNettyAllocatorMetrics(ByteBufAllocator.DEFAULT);
        for (final var group : eventGroups) {
            Metrics.bindNettyEventExecutorMetrics(group.bossGroup(), group.workerGroup());
        }
    }

    private void initVersionInfoMetric() {
        Metrics.versionInfoMetric(VersionInfo.VERSION_INFO);
    }

    private Map<ApiKeys, Short> getApiKeyMaxVersionOverride(Configuration config) {
        Map<String, Number> apiKeyIdMaxVersion = extractApiVersionOverrides(config);

        return apiKeyIdMaxVersion.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> ApiKeys.valueOf(e.getKey()),
                        e -> e.getValue().shortValue()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Number> extractApiVersionOverrides(Configuration config) {
        return config.development()
                .map(m -> m.get("apiKeyIdMaxVersionOverride"))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .orElse(Map.of());
    }

    private ServerBootstrap buildServerBootstrap(EventGroupConfig virtualHostEventGroup, KafkaProxyInitializer kafkaProxyInitializer) {
        return new ServerBootstrap()
                .group(virtualHostEventGroup.bossGroup(), virtualHostEventGroup.workerGroup())
                .channel(virtualHostEventGroup.clazz())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(kafkaProxyInitializer)
                .childOption(ChannelOption.TCP_NODELAY, true);
    }

    @SuppressWarnings("resource") // suppressing resource as ExecutorService is not closeable in Java 17 (our runtime target)
    private CompletableFuture<Void> maybeStartManagementListener(EventGroupConfig eventGroupConfig, MeterRegistries meterRegistries) {
        return Optional.ofNullable(managementConfiguration)
                .map(mc -> {
                    var metricsBootstrap = new ServerBootstrap().group(eventGroupConfig.bossGroup(), eventGroupConfig.workerGroup())
                            .option(ChannelOption.SO_REUSEADDR, true)
                            .channel(eventGroupConfig.clazz())
                            .childHandler(new ManagementInitializer(meterRegistries, mc));
                    LOGGER.atInfo()
                            .addKeyValue("bindAddress", mc.getEffectiveBindAddress())
                            .addKeyValue("port", mc.getEffectivePort())
                            .log("Binding management endpoint");

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
     * Apply the given configuration to this running proxy, restarting only the virtual clusters
     * whose effective configuration differs from the current running state. Unaffected clusters
     * continue serving traffic throughout the reconfigure.
     *
     * <p>See {@link ConfigurationReloadOrchestrator} for the full pipeline shape (pre-flight
     * static-section diff, concurrency control, change detection, per-VC execution,
     * result construction).
     *
     *
     * @param newConfig the desired end-state configuration; must be non-null
     * @return a future that completes with the reconfigure outcome
     * @throws NullPointerException  if {@code newConfig} is {@code null}
     * @throws IllegalStateException if the proxy is not in the running state
     */
    public CompletableFuture<ReconfigureResult> reconfigure(Configuration newConfig) {
        Objects.requireNonNull(newConfig, "newConfig");
        if (state.get() != LifecycleState.STARTED) {
            throw new IllegalStateException("This proxy is not running");
        }
        ConfigurationReloadOrchestrator orchestrator = reconfigureOrchestrator;
        if (orchestrator == null) {
            throw new IllegalStateException("Reconfigure orchestrator has not been initialised");
        }
        return orchestrator.reconfigure(newConfig);
    }

    /**
     * Shuts down a running proxy. Idempotent: safe to call from any thread, including JVM shutdown hooks,
     * and safe to call before {@link #startup()} or after already stopped.
     */
    public void shutdown() {
        transitionTo(LifecycleState.STOPPING, this::doShutdown, lifecycleState -> {
        });
    }

    private void doShutdown() {
        Exception shutdownFailure = null;
        try {
            STARTUP_SHUTDOWN_LOGGER.atInfo()
                    .log("Shutting down");

            // Unbind ports first so no new connections can arrive after the drain snapshot is taken —
            // existing connections are unaffected and will be drained in the next step.
            unbindPorts();

            shutdownVirtualClusters();

            shutdownNetty();

            if (filterChainFactory != null) {
                filterChainFactory.close();
            }
            if (meterRegistries != null) {
                meterRegistries.close();
            }
        }
        catch (Exception e) {
            shutdownFailure = e;
        }
        finally {
            // Close virtual cluster models to release TLS credential supplier resources
            virtualClusterModels.forEach(VirtualClusterModel::close);
            transitionTo(LifecycleState.STOPPED, () -> {
            }, lifecycleState -> {
                throw new LifecycleException("failed to transition to stopped from state: " + lifecycleState);
            });
            managementEventGroup = null;
            proxyEventGroup = null;
            meterRegistries = null;
            filterChainFactory = null;
            reconfigureOrchestrator = null;
            if (shutdownFailure != null) {
                shutdown.completeExceptionally(shutdownFailure);
            }
            else {
                shutdown.complete(null);
            }
            LOGGER.atInfo()
                    .log("Shut down completed");
        }
    }

    private void unbindPorts() {
        endpointRegistry.shutdown().handle((u, t) -> {
            bindingOperationProcessor.close();
            if (t != null) {
                STARTUP_SHUTDOWN_LOGGER.atWarn()
                        .setCause(t)
                        .log("Shutdown completed exceptionally");
                throw new LifecycleException("Shutdown completed exceptionally", t);
            }
            return null;
        }).toCompletableFuture().join();
    }

    private void shutdownVirtualClusters() {
        try {
            virtualClusterRegistry.shutdownAllClusters();
            STARTUP_SHUTDOWN_LOGGER.atInfo().log("All connections drained successfully");
        }
        catch (Exception e) {
            STARTUP_SHUTDOWN_LOGGER.atWarn()
                    .addKeyValue("error", e.getMessage())
                    .log("Connection drain completed with errors — Netty shutdown will force-close remaining");
        }
    }

    private void shutdownNetty() {
        var closeFutures = new ArrayList<Future<?>>();
        if (proxyEventGroup != null) {
            closeFutures.addAll(proxyEventGroup.shutdownGracefully());
        }
        if (managementEventGroup != null) {
            closeFutures.addAll(managementEventGroup.shutdownGracefully());
        }
        closeFutures.forEach(Future::syncUninterruptibly);
    }

    /**
     * Returns the lifecycle manager for the given virtual cluster name.
     * @param clusterName the virtual cluster name
     * @return the lifecycle manager, or null if no cluster with that name exists
     */
    @VisibleForTesting
    @Nullable
    VirtualClusterLifecycle lifecycleFor(String clusterName) {
        return virtualClusterRegistry.lifecycleFor(clusterName);
    }

    /**
     * Returns the actual local port that the proxy is listening on for the given bind address and configured port.
     * Useful when the configured port is {@link EndpointRegistry#OS_ASSIGNED_PORT} (meaning the OS assigns an ephemeral port at startup).
     * Must only be called after a successful {@link #startup()}.
     *
     * @param bindAddress the bind address used in the proxy configuration, or {@code null} for any-address bindings
     * @param port the port number used in the proxy configuration (e.g. {@link EndpointRegistry#OS_ASSIGNED_PORT} for OS-assigned)
     * @return the actual local port the proxy is listening on
     */
    @VisibleForTesting
    int listeningPort(@Nullable String bindAddress, int port) {
        return endpointRegistry.localPortFor(Endpoint.createEndpoint(Optional.ofNullable(bindAddress), port, false));
    }

    @Override
    public void close() {
        shutdown();
    }

}
