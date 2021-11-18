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
import java.util.function.Function;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.strimzi.kproxy.interceptor.AdvertisedListenersInterceptor;
import io.strimzi.kproxy.interceptor.ApiVersionsInterceptor;
import io.strimzi.kproxy.interceptor.Interceptor;
import io.strimzi.kproxy.interceptor.InterceptorProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class KafkaProxy {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxy.class);

    public static void main(String[] args) throws Exception {
        run(null,
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
                        })//,
//                        new Interceptor() {
//                            @Override
//                            public ChannelInboundHandler frontendHandler() {
//                                return null;
//                            }
//
//                            @Override
//                            public ChannelInboundHandler backendHandler() {
//                                return null;
//                            }
//
//                            @Override
//                            public boolean shouldDecodeRequest(ApiKeys apiKey, int apiVersion) {
//                                return true;
//                            }
//
//                            @Override
//                            public boolean shouldDecodeResponse(ApiKeys apiKey, int apiVersion) {
//                                return true;
//                            }
//                        }
                )
        );
    }

    public static void run(
                           String localHost,
                           int localPort,
                           String remoteHost,
                           int remotePort,
                           boolean logNetwork,
                           boolean logFrames,
                           List<Interceptor> interceptors) throws Exception {
        LOGGER.info("Proxying *:" + localPort + " to " + remoteHost + ':' + remotePort + " ...");

        Function<SocketChannel, InterceptorProvider> hpp = ch -> new InterceptorProvider(interceptors);

        // Configure the bootstrap.
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new KafkaProxyInitializer(remoteHost, remotePort, hpp, logNetwork, logFrames))
                    .childOption(ChannelOption.AUTO_READ, false)
                    .bind(localHost, localPort).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}
