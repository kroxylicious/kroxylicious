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
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;

import io.kroxylicious.proxy.bootstrap.ClusterEndpointProviderFactory;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.internal.KafkaProxyInitializer;
import io.kroxylicious.proxy.internal.MeterRegistries;
import io.kroxylicious.proxy.internal.admin.AdminHttpInitializer;
import io.kroxylicious.proxy.internal.filter.FixedNetFilter;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;

import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_REQUEST_SIZE_BYTES;

public final class KafkaProxy implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxy.class);

    private final String proxyHost;
    private final int proxyPort;
    private final String brokerHost;
    private final int brokerPort;
    private final boolean logNetwork;
    private final boolean logFrames;
    private final boolean useIoUring;
    private final FilterChainFactory filterChainFactory;
    private final AdminHttpConfiguration adminHttpConfig;
    private final List<MicrometerDefinition> micrometerConfig;
    private final ClusterEndpointProvider endpointProvider;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel acceptorChannel;
    private Channel metricsChannel;
    private Optional<File> keyStoreFile;
    private Optional<String> keyStorePassword;

    public KafkaProxy(Configuration config) {
        var virtualCluster = config.virtualClusters().entrySet().iterator().next().getValue();
        var endpointProvider = new ClusterEndpointProviderFactory(config, virtualCluster).createClusterEndpointProvider();
        var proxyAddress = endpointProvider.getClusterBootstrapAddress();
        var proxyAddressParts = proxyAddress.split(":");

        // TODO: deal with list
        var targetBootstrapServers = virtualCluster.targetCluster().bootstrapServers();
        var targetBootstrapServersParts = targetBootstrapServers.split(",");
        var brokerAddressParts = targetBootstrapServersParts[0].split(":");

        this.proxyHost = proxyAddressParts[0];
        this.proxyPort = Integer.valueOf(proxyAddressParts[1]);
        this.brokerHost = brokerAddressParts[0];
        this.brokerPort = Integer.valueOf(brokerAddressParts[1]);
        this.logNetwork = virtualCluster.isLogNetwork();
        this.logFrames = virtualCluster.isLogFrames();
        this.useIoUring = virtualCluster.isUseIoUring();
        this.adminHttpConfig = config.adminHttpConfig();
        this.micrometerConfig = config.getMicrometer();
        this.endpointProvider = endpointProvider;
        this.filterChainFactory = new FilterChainFactory(config, endpointProvider);

        this.keyStoreFile = virtualCluster.keyStoreFile().map(File::new);
        this.keyStorePassword = virtualCluster.keyPassword();
    }

    public String proxyHost() {
        return proxyHost;
    }

    public int proxyPort() {
        return proxyPort;
    }

    public String proxyAddress() {
        return proxyHost() + ":" + proxyPort();
    }

    public String brokerHost() {
        return brokerHost;
    }

    public int brokerPort() {
        return brokerPort;
    }

    public String brokerAddress() {
        return brokerHost() + ":" + brokerPort();
    }

    public boolean useIoUring() {
        return useIoUring;
    }

    /**
     * Starts this proxy.
     * @return This proxy.
     */
    public KafkaProxy startup() throws InterruptedException {
        if (acceptorChannel != null) {
            throw new IllegalStateException("This proxy is already running");
        }

        LOGGER.info("Proxying local {} to remote {}",
                proxyAddress(), brokerAddress());

        Optional<SslContext> sslContext = keyStoreFile.map(ksf -> {
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
                logNetwork,
                logFrames,
                sslContext);

        final int availableCores = Runtime.getRuntime().availableProcessors();

        // Configure the bootstrap.
        final Class<? extends ServerChannel> channelClass;
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

        MeterRegistries meterRegistries = new MeterRegistries(micrometerConfig, endpointProvider);
        maybeStartMetricsListener(bossGroup, workerGroup, channelClass, meterRegistries);

        ServerBootstrap serverBootstrap = new ServerBootstrap().group(bossGroup, workerGroup)
                .channel(channelClass)
                .childHandler(initializer)
                .childOption(ChannelOption.AUTO_READ, false)
                .childOption(ChannelOption.TCP_NODELAY, true);
        ChannelFuture bindFuture;
        if (proxyHost != null) {
            bindFuture = serverBootstrap.bind(proxyHost, proxyPort);
        }
        else {
            bindFuture = serverBootstrap.bind(proxyPort);
        }
        acceptorChannel = bindFuture.sync().channel();
        // Pre-register counters/summaries to avoid creating them on first request and thus skewing the request latency
        Metrics.counter(KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES);
        Metrics.counter(KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES);
        Metrics.summary(KROXYLICIOUS_REQUEST_SIZE_BYTES);
        return this;
    }

    private void maybeStartMetricsListener(EventLoopGroup bossGroup,
                                           EventLoopGroup workerGroup,
                                           Class<? extends ServerChannel> channelClass,
                                           MeterRegistries meterRegistries)
            throws InterruptedException {
        if (adminHttpConfig != null
                && adminHttpConfig.getEndpoints().maybePrometheus().isPresent()) {
            ServerBootstrap metricsBootstrap = new ServerBootstrap().group(bossGroup, workerGroup)
                    .channel(channelClass)
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
        if (acceptorChannel == null) {
            throw new IllegalStateException("This proxy is not running");
        }
        acceptorChannel.closeFuture().sync();
    }

    /**
     * Shuts down a running proxy.
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        if (acceptorChannel == null) {
            throw new IllegalStateException("This proxy is not running");
        }
        bossGroup.shutdownGracefully().sync();
        workerGroup.shutdownGracefully().sync();
        bossGroup = null;
        workerGroup = null;
        acceptorChannel = null;
        metricsChannel = null;
    }

    @Override
    public void close() throws Exception {
        if (acceptorChannel != null) {
            shutdown();
        }
    }
}
