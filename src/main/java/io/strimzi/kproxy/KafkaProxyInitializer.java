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

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.strimzi.kproxy.codec.Correlation;
import io.strimzi.kproxy.codec.KafkaRequestDecoder;
import io.strimzi.kproxy.codec.KafkaResponseEncoder;
import io.strimzi.kproxy.interceptor.InterceptorProvider;
import io.strimzi.kproxy.interceptor.InterceptorProviderFactory;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyInitializer.class);

    private final String remoteHost;
    private final int remotePort;
    private final InterceptorProviderFactory interceptorProviderFactory;
    private final boolean logNetwork;
    private final boolean logFrames;

    public KafkaProxyInitializer(String remoteHost,
                                 int remotePort,
                                 InterceptorProviderFactory interceptorProviderFactory,
                                 boolean logNetwork,
                                 boolean logFrames) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.interceptorProviderFactory = interceptorProviderFactory;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        // TODO TLS

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        var correlation = new HashMap<Integer, Correlation>();

        InterceptorProvider interceptorProvider = interceptorProviderFactory.createInterceptorProvider(ch);
        if (logNetwork) {
            ch.pipeline().addLast(new LoggingHandler("frontend-network", LogLevel.INFO));
        }
        ch.pipeline().addLast(new KafkaRequestDecoder(interceptorProvider, correlation));

        for (ChannelInboundHandler requestInterceptor : interceptorProvider.frontendHandlers()) {
            ch.pipeline().addLast(requestInterceptor);
        }

        ch.pipeline().addLast(new KafkaResponseEncoder());
        if (logFrames) {
            ch.pipeline().addLast(new LoggingHandler("frontend-application", LogLevel.INFO));
        }
        ch.pipeline().addLast(new KafkaProxyFrontendHandler(remoteHost, remotePort, correlation, interceptorProvider, logNetwork, logFrames));
    }
}
