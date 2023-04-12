/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.KeyManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Metrics;
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
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.bootstrap.ClusterEndpointProviderFactory;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.admin.AdminHttpInitializer;
import io.kroxylicious.proxy.internal.filter.FixedNetFilter;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_REQUEST_SIZE_BYTES;

public final class KafkaProxy implements AutoCloseable {

    private record EventGroupConfig(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> clazz) {
        public List<Future<?>> shutdownGracefully() {
            return List.of(bossGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }
    };

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

        virtualClusterMap.forEach((name, virtualCluster) -> {

            var endpointProvider = new ClusterEndpointProviderFactory(config, virtualCluster).createClusterEndpointProvider();

            // TODO: target bootstrap servers could be a list - we should be prepared to try them all.
            var targetBootstrapServers = virtualCluster.targetCluster().bootstrapServers();
            var targetBootstrapServersParts = targetBootstrapServers.split(",");
            var brokerAddressParts = targetBootstrapServersParts[0].split(":");
            var brokerHost = brokerAddressParts[0];
            var brokerPort = Integer.valueOf(brokerAddressParts[1]);

            var keyStoreFile = virtualCluster.keyStoreFile().map(File::new);
            var keyStorePassword = virtualCluster.keyPassword();

            var filterChainFactory = new FilterChainFactory(config, endpointProvider);

            LOGGER.info("{}: Proxying local {} to remote {}",
                    name, endpointProvider.getClusterBootstrapAddress(), targetBootstrapServers);

            var sslContext = keyStoreFile.map(ksf -> {
                try (var is = new FileInputStream(ksf)) {
                    var password = keyStorePassword.map(String::toCharArray).orElse(null);
                    var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    keyStore.load(is, password);
                    var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    keyManagerFactory.init(keyStore, password);
                    return SslContextBuilder.forServer(keyManagerFactory).build();
                }
                catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException e) {
                    throw new RuntimeException(e);
                }
            });

            KafkaProxyInitializer initializer = new KafkaProxyInitializer(false,
                    Map.of(),
                    new FixedNetFilter(brokerHost,
                            brokerPort,
                            filterChainFactory),
                    virtualCluster.isLogNetwork(),
                    virtualCluster.isLogFrames(),
                    sslContext);

            // Configure the bootstrap.
            var virtualHostEventGroup = buildNettyEventGroups(availableCores, virtualCluster.isUseIoUring());
            this.virtualHostEventGroups.add(virtualHostEventGroup);

            ServerBootstrap serverBootstrap = new ServerBootstrap().group(virtualHostEventGroup.bossGroup(), virtualHostEventGroup.workerGroup())
                    .channel(virtualHostEventGroup.clazz())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childHandler(initializer)
                    .childOption(ChannelOption.AUTO_READ, false)
                    .childOption(ChannelOption.TCP_NODELAY, true);
            ChannelFuture bindFuture;

            var toBind = new HashSet<HostPort>();
            toBind.add(endpointProvider.getClusterBootstrapAddress());
            for (int i = 0; i < endpointProvider.getNumberOfBrokerEndpointsToPrebind(); i++) {
                toBind.add(endpointProvider.getBrokerAddress(i));
            }

            for (var address : toBind) {
                var proxyPort = address.port();

                if (endpointProvider.getBindAddress().isPresent()) {
                    bindFuture = serverBootstrap.bind(endpointProvider.getBindAddress().get(), proxyPort);
                }
                else {
                    bindFuture = serverBootstrap.bind(proxyPort);
                }

                try {
                    var channel = bindFuture.sync().channel();
                    acceptorChannels.add(channel);
                    LOGGER.debug("{}: Listener started {}", name, channel.localAddress());
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        });

        // Pre-register counters/summaries to avoid creating them on first request and thus skewing the request latency
        // TODO add a virtual host tag to metrics
        Metrics.counter(KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES);
        Metrics.counter(KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES);
        Metrics.summary(KROXYLICIOUS_REQUEST_SIZE_BYTES);
        return this;
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
}
