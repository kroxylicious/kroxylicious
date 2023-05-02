/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.VirtualClusterResolutionException;
import io.kroxylicious.proxy.internal.VirtualClusterResolver;
import io.kroxylicious.proxy.internal.admin.AdminHttpInitializer;
import io.kroxylicious.proxy.internal.net.NetworkBinding;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public final class KafkaProxy implements AutoCloseable, VirtualClusterResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxy.class);

    private Map<ClusterEndpointConfigProvider, VirtualCluster> endpointProviders;

    private record EventGroupConfig(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz) {
        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }
    };

    private final Configuration config;
    private final AdminHttpConfiguration adminHttpConfig;
    private final List<MicrometerDefinition> micrometerConfig;
    private final Map<String, VirtualCluster> virtualClusterMap;
    private final Queue<Channel> acceptorChannels = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private EventGroupConfig adminEventGroup;
    private EventGroupConfig serverEventGroup;
    private Channel metricsChannel;

    public KafkaProxy(Configuration config) {
        this.config = config;
        this.virtualClusterMap = config.virtualClusters();
        this.adminHttpConfig = config.adminHttpConfig();
        this.micrometerConfig = config.getMicrometer();
    }

    /**
     * Starts this proxy.
     * @return This proxy.
     */
    public KafkaProxy startup() throws InterruptedException {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("This proxy is already running");
        }

        var availableCores = Runtime.getRuntime().availableProcessors();
        var meterRegistries = new MeterRegistries(micrometerConfig);

        this.adminEventGroup = buildNettyEventGroups(availableCores, config.isUseIoUring());
        this.serverEventGroup = buildNettyEventGroups(availableCores, config.isUseIoUring());

        maybeStartMetricsListener(adminEventGroup, meterRegistries);

        this.endpointProviders = virtualClusterMap.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().getClusterEndpointProvider(), Map.Entry::getValue));

        var networkBindings = buildNetworkBindings();

        var tlsServerBootstrap = buildServerBootstrap(serverEventGroup, new KafkaProxyInitializer(config, true, this, false, Map.of()));
        var plainServerBootstrap = buildServerBootstrap(serverEventGroup, new KafkaProxyInitializer(config, false, this, false, Map.of()));

        var bindFutures = networkBindings.stream()
                .map(networkBinder -> networkBinder.bind(networkBinder.tls() ? tlsServerBootstrap : plainServerBootstrap))
                .collect(Collectors.toSet());
        bindFutures.forEach(channelFuture -> channelFuture.addListener(f -> LOGGER.info("Listening : {}", channelFuture.channel().localAddress())));
        bindFutures.forEach(ChannelFuture::syncUninterruptibly);

        // Pre-register counters/summaries to avoid creating them on first request and thus skewing the request latency
        // TODO add a virtual host tag to metrics
        Metrics.inboundDownstreamMessagesCounter();
        Metrics.inboundDownstreamDecodedMessagesCounter();
        return this;
    }

    private Set<NetworkBinding> buildNetworkBindings() {
        var exclusivePortBindings = new HashSet<Integer>();
        var allBindings = new HashSet<NetworkBinding>();

        virtualClusterMap.forEach((name, virtualCluster) -> {
            var endpointProvider = virtualCluster.getClusterEndpointProvider();

            var useTls = virtualCluster.isUseTls();
            if (endpointProvider.requiresTls() && !useTls) {
                throw new IllegalStateException("Endpoint configuration for virtual cluster %s requires TLS, but no TLS configuration is specified".formatted(name));
            }

            var toBind = new HashSet<HostPort>();
            toBind.add(endpointProvider.getClusterBootstrapAddress());
            for (int i = 0; i < endpointProvider.getNumberOfBrokerEndpointsToPrebind(); i++) {
                toBind.add(endpointProvider.getBrokerAddress(i));
            }

            var bindAddress = endpointProvider.getBindAddress();
            var virtualHostEndpoints = toBind.stream()
                    .map(hp -> NetworkBinding.createNetworkBinding(bindAddress, hp.port(), useTls))
                    .collect(Collectors.toSet());

            var virtualClusterPorts = virtualHostEndpoints.stream().map(NetworkBinding::port).collect(Collectors.toSet());
            checkForPortExclusivityConflicts(exclusivePortBindings, virtualClusterPorts);
            if (endpointProvider.requiresPortExclusivity()) {
                exclusivePortBindings.addAll(virtualClusterPorts);
            }
            allBindings.addAll(virtualHostEndpoints);
        });
        return allBindings;
    }

    private void checkForPortExclusivityConflicts(Set<Integer> exclusivePortBindings, Set<Integer> virtualClusterPorts) {
        var conflicts = exclusivePortBindings.stream().filter(virtualClusterPorts::contains).map(String::valueOf).collect(Collectors.joining(","));
        if (!conflicts.isBlank()) {
            throw new IllegalStateException("Configuration produces port conflict(s) : " + conflicts);
        }
    }

    private ServerBootstrap buildServerBootstrap(EventGroupConfig virtualHostEventGroup, KafkaProxyInitializer kafkaProxyInitializer) {
        return new ServerBootstrap().group(virtualHostEventGroup.bossGroup(), virtualHostEventGroup.workerGroup())
                .channel(virtualHostEventGroup.clazz())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(kafkaProxyInitializer)
                .childOption(ChannelOption.TCP_NODELAY, true);
    }

    private EventGroupConfig buildNettyEventGroups(int availableCores, boolean useIoUring) {
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
        return new EventGroupConfig(bossGroup, workerGroup, channelClass);
    }

    private void maybeStartMetricsListener(EventGroupConfig eventGroupConfig,
                                           MeterRegistries meterRegistries)
            throws InterruptedException {
        if (adminHttpConfig != null
                && adminHttpConfig.getEndpoints().maybePrometheus().isPresent()) {
            ServerBootstrap metricsBootstrap = new ServerBootstrap().group(eventGroupConfig.bossGroup(), eventGroupConfig.workerGroup())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .channel(eventGroupConfig.clazz())
                    .childHandler(new AdminHttpInitializer(meterRegistries, adminHttpConfig));
            metricsChannel = metricsBootstrap.bind(adminHttpConfig.getHost(), adminHttpConfig.getPort()).sync().channel();
        }
    }

    /**
     * Blocks while this proxy is running.
     * This should only be called after a successful call to {@link #startup()}.
     * @throws InterruptedException
     */
    public void block() throws InterruptedException {
        if (!running.get()) {
            throw new IllegalStateException("This proxy is not running");
        }
        while (!acceptorChannels.isEmpty()) {
            Channel channel = acceptorChannels.peek();
            if (channel != null) {
                channel.closeFuture().sync();
            }
        }
    }

    /**
     * Shuts down a running proxy.
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        if (!running.getAndSet(false)) {
            throw new IllegalStateException("This proxy is not running");
        }
        var closeFutures = new ArrayList<Future<?>>();
        closeFutures.addAll(serverEventGroup.shutdownGracefully());
        closeFutures.addAll(adminEventGroup.shutdownGracefully());
        closeFutures.forEach(Future::syncUninterruptibly);
        acceptorChannels.clear();
        adminEventGroup = null;
        serverEventGroup = null;
        metricsChannel = null;
    }

    @Override
    public void close() throws Exception {
        if (running.get()) {
            shutdown();
        }
    }

    @Override
    public VirtualCluster resolve(String sniHostname, int targetPort) {
        var matchingVirtualClusters = endpointProviders.entrySet().stream()
                .filter(e -> e.getKey().hasMatchingEndpoint(sniHostname, targetPort).matched()).map(Map.Entry::getValue).toList();
        // We only expect one match
        if (matchingVirtualClusters.isEmpty()) {
            throw new VirtualClusterResolutionException("Failed to find matching virtual cluster for %s on port %d".formatted(sniHostname, targetPort));
        }
        else if (matchingVirtualClusters.size() > 1) {
            throw new VirtualClusterResolutionException("Found too many virtual cluster matches for %s on port %d".formatted(sniHostname, targetPort));
        }
        return matchingVirtualClusters.get(0);
    }

    @Override
    public VirtualCluster resolve(int targetPort) {
        var matchingVirtualClusters = endpointProviders.entrySet().stream()
                .filter(e -> e.getKey().hasMatchingEndpoint(null, targetPort).matched()).map(Map.Entry::getValue).toList();
        // We only expect one match
        if (matchingVirtualClusters.isEmpty()) {
            throw new VirtualClusterResolutionException("Failed to find matching virtual cluster for port %d".formatted(targetPort));
        }
        else if (matchingVirtualClusters.size() > 1) {
            throw new VirtualClusterResolutionException("Found too many virtual cluster matches for port %d".formatted(targetPort));
        }
        return matchingVirtualClusters.get(0);
    }
}
