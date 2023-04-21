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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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

import io.kroxylicious.proxy.bootstrap.ClusterEndpointProviderFactory;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.VirtualClusterResolver;
import io.kroxylicious.proxy.internal.admin.AdminHttpInitializer;
import io.kroxylicious.proxy.internal.net.NetworkBinding;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

public final class KafkaProxy implements AutoCloseable, VirtualClusterResolver {

    private Map<ClusterEndpointConfigProvider, VirtualCluster> endpointProviders;

    private record EventGroupConfig(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz) {
        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }
    }

    ;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxy.class);

    private final Configuration config;
    private final AdminHttpConfiguration adminHttpConfig;
    private final List<MicrometerDefinition> micrometerConfig;
    private final Map<String, VirtualCluster> virtualClusterMap;
    private EventGroupConfig adminEventGroup;
    private final List<EventGroupConfig> virtualHostEventGroups = new CopyOnWriteArrayList<>();

    private final Queue<Channel> acceptorChannels = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
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
        this.adminEventGroup = buildNettyEventGroups(availableCores, false);
        maybeStartMetricsListener(adminEventGroup, meterRegistries);

        // TODO make clusterProvider a property of the virtualcluster
        this.endpointProviders = virtualClusterMap.entrySet().stream()
                .collect(Collectors.toMap(e -> new ClusterEndpointProviderFactory(e.getValue().clusterEndpointProvider()).createClusterEndpointProvider(),
                        Map.Entry::getValue));

        var networkBinders = new HashSet<NetworkBinding>();

        virtualClusterMap.forEach((name, virtualCluster) -> {
            var endpointProvider = virtualCluster.getClusterEndpointProvider();

            var toBind = new HashSet<HostPort>();
            toBind.add(endpointProvider.getClusterBootstrapAddress());
            for (int i = 0; i < endpointProvider.getNumberOfBrokerEndpointsToPrebind(); i++) {
                toBind.add(endpointProvider.getBrokerAddress(i));
            }

            networkBinders.addAll(toBind.stream().map(hp -> NetworkBinding.createNetworkBinding(endpointProvider.getBindAddress(), hp.port(), virtualCluster.isUseTls()))
                    .collect(Collectors.toSet()));
        });

        var virtualHostEventGroup = buildNettyEventGroups(availableCores, false /* FIXME */);
        this.virtualHostEventGroups.add(virtualHostEventGroup);

        var tlsServerBootstrap = buildServerBootstrap(virtualHostEventGroup, new KafkaProxyInitializer(config, true, this, false, Map.of()));
        var plainServerBootstrap = buildServerBootstrap(virtualHostEventGroup, new KafkaProxyInitializer(config, false, this, false, Map.of()));

        var bindFutures = networkBinders.stream()
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

    private ServerBootstrap buildServerBootstrap(EventGroupConfig virtualHostEventGroup, KafkaProxyInitializer kafkaProxyInitializer) {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(virtualHostEventGroup.bossGroup(), virtualHostEventGroup.workerGroup())
                .channel(virtualHostEventGroup.clazz())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(kafkaProxyInitializer)
                .childOption(ChannelOption.TCP_NODELAY, true);
        return serverBootstrap;
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
        virtualHostEventGroups.forEach(g -> closeFutures.addAll(g.shutdownGracefully()));
        closeFutures.addAll(adminEventGroup.shutdownGracefully());
        closeFutures.forEach(Future::syncUninterruptibly);
        virtualHostEventGroups.clear();
        acceptorChannels.clear();
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
            throw new IllegalStateException("Failed to find matching virtual cluster for %s on port %d".formatted(sniHostname, targetPort));
        }
        else if (matchingVirtualClusters.size() > 1) {
            throw new IllegalStateException("Found too many virtual cluster matches for %s on port %d".formatted(sniHostname, targetPort));
        }
        return matchingVirtualClusters.get(0);
    }

    @Override
    public VirtualCluster resolve(int targetPort) {
        var matchingVirtualClusters = endpointProviders.entrySet().stream()
                .filter(e -> e.getKey().hasMatchingEndpoint(null, targetPort).matched()).map(Map.Entry::getValue).toList();
        // We only expect one match
        if (matchingVirtualClusters.isEmpty()) {
            throw new IllegalStateException("Failed to find matching virtual cluster for port %d".formatted(targetPort));
        }
        else if (matchingVirtualClusters.size() > 1) {
            throw new IllegalStateException("Found too many virtual cluster matches for port %d".formatted(targetPort));
        }
        return matchingVirtualClusters.get(0);
    }
}
