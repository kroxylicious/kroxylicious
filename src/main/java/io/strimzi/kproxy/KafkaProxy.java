/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.strimzi.kproxy.interceptor.AdvertisedListenersInterceptor;
import io.strimzi.kproxy.interceptor.ApiVersionsInterceptor;
import io.strimzi.kproxy.interceptor.Interceptor;
import io.strimzi.kproxy.interceptor.InterceptorProviderFactory;

public final class KafkaProxy {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxy.class);
    private final String proxyHost;
    private final int proxyPort;
    private final String brokerHost;
    private final int brokerPort;
    private final boolean logNetwork;
    private final boolean logFrames;
    private final List<Interceptor> interceptors;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private Channel acceptorChannel;

    public static void main(String[] args) throws Exception {
        new KafkaProxy(null,
                Integer.parseInt(System.getProperty("localPort", "9192")),
                System.getProperty("remoteHost", "localhost"),
                Integer.parseInt(System.getProperty("remotePort", "9092")),
                false,
                false,
                List.of(
                        new ApiVersionsInterceptor(),
                        new AdvertisedListenersInterceptor(new AdvertisedListenersInterceptor.AddressMapping() {
                            @Override
                            public String host(String host, int port) {
                                return host;
                            }

                            @Override
                            public int port(String host, int port) {
                                return port + 100;
                            }
                        })))
                                .startup()
                                .block();
    }

    public KafkaProxy(
                      String proxyHost,
                      int proxyPort,
                      String brokerHost,
                      int brokerPort,
                      boolean logNetwork,
                      boolean logFrames,
                      List<Interceptor> interceptors) {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.interceptors = interceptors;
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

        InterceptorProviderFactory interceptorProviderFactory = new InterceptorProviderFactory(interceptors);
        KafkaProxyInitializer initializer = new KafkaProxyInitializer(brokerHost, brokerPort, interceptorProviderFactory, logNetwork, logFrames);

        // Configure the bootstrap.
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(initializer)
                .childOption(ChannelOption.AUTO_READ, false);
        ChannelFuture bindFuture;
        if (proxyHost != null) {
            bindFuture = serverBootstrap.bind(proxyHost, proxyPort);
        }
        else {
            bindFuture = serverBootstrap.bind(proxyPort);
        }
        acceptorChannel = bindFuture.sync().channel();
        //
        return this;
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
    }

}
