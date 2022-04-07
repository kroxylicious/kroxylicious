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
import java.util.function.Function;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.strimzi.kproxy.codec.Correlation;
import io.strimzi.kproxy.codec.KafkaRequestDecoder;
import io.strimzi.kproxy.codec.KafkaResponseEncoder;
import io.strimzi.kproxy.interceptor.InterceptorProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyInitializer.class);

    private final String remoteHost;
    private final int remotePort;
    private final Function<SocketChannel, InterceptorProvider> hpp;
    private final boolean logNetwork;
    private final boolean logFrames;

    public KafkaProxyInitializer(String remoteHost,
                                 int remotePort,
                                 Function<SocketChannel, InterceptorProvider> hpp,
                                 boolean logNetwork,
                                 boolean logFrames) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.hpp = hpp;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        // TODO TLS

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        var correlation = new HashMap<Integer, Correlation>();

        InterceptorProvider hp = hpp.apply(ch);
        if (logNetwork) {
            ch.pipeline().addLast(new LoggingHandler("frontend-network", LogLevel.INFO));
        }
        ch.pipeline().addLast(
                new KafkaRequestDecoder(hp, correlation)
        );
        var frontendInterceptor = hp.frontendHandlers();
        if (frontendInterceptor != null){
            frontendInterceptor.forEach(handler -> ch.pipeline().addLast(handler));
        }
        ch.pipeline().addLast(new KafkaResponseEncoder());
        if (logFrames) {
            ch.pipeline().addLast(new LoggingHandler("frontend-application", LogLevel.INFO));
        }
        ch.pipeline().addLast(new KafkaProxyFrontendHandler(remoteHost, remotePort, correlation, hp, logNetwork, logFrames));
    }
}