/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.net;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * A Proxy server that listens on a random ephemeral port, and proxies all data received to
 * a remote host/port. This is to enable us to test cases where Kroxylicious sits behind
 * yet another proxy using a different port scheme, and we need the clients to be told
 * how to connect to the outer proxy.
 */
public class PassthroughProxy implements Closeable {
    private final ChannelFuture future;
    private static final Logger LOGGER = LoggerFactory.getLogger(PassthroughProxy.class);

    public PassthroughProxy(int remotePort, String remoteHost) {
        ServerBootstrap b = new ServerBootstrap();
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        future = b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new PassthroughProxyInitiailizer(remoteHost, remotePort))
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(0);
    }

    public int getLocalPort() {
        try {
            future.get(5, TimeUnit.SECONDS);
            SocketAddress socketAddress = future.channel().localAddress();
            if (socketAddress instanceof InetSocketAddress) {
                return ((InetSocketAddress) socketAddress).getPort();
            }
            else {
                throw new RuntimeException("Unexpected socket address type: " + socketAddress.toString());
            }
        }
        catch (Exception e) {
            LOGGER.error("channel future did not complete within 5 seconds");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        future.addListener(
                (ChannelFutureListener) channelFuture -> channelFuture.channel().close()
                        .addListener((ChannelFutureListener) channelFuture1 -> LOGGER.info("passthrough proxy closed")));
    }

    private static class PassthroughProxyInitiailizer extends ChannelInitializer<SocketChannel> {
        private final String remoteHost;
        private final int remotePort;

        private PassthroughProxyInitiailizer(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
        }

        @Override
        public void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(new PassthroughProxyFrontendHandler(remoteHost, remotePort));
        }

    }

    private static class PassthroughProxyFrontendHandler extends ChannelInboundHandlerAdapter {
        private final String remoteHost;
        private final int remotePort;

        private PassthroughProxyFrontendHandler(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
        }

        private Channel outboundChannel;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            final Channel inboundChannel = ctx.channel();

            Bootstrap b = new Bootstrap();
            b.group(inboundChannel.eventLoop())
                    .channel(ctx.channel().getClass())
                    .handler(new PassthroughProxyBackendHandler(inboundChannel))
                    .option(ChannelOption.AUTO_READ, false);
            ChannelFuture f = b.connect(remoteHost, remotePort);
            outboundChannel = f.channel();
            f.addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    inboundChannel.read();
                }
                else {
                    inboundChannel.close();
                }
            });
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isSuccess()) {
                        ctx.channel().read();
                    }
                    else {
                        channelFuture.channel().close();
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (outboundChannel != null) {
                closeOnFlush(outboundChannel);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.info("exception caught in frontend handler", cause);
            closeOnFlush(ctx.channel());
        }

        /**
         * Closes the specified channel after all queued write requests are flushed.
         */
        static void closeOnFlush(Channel ch) {
            if (ch.isActive()) {
                ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    private static class PassthroughProxyBackendHandler extends ChannelInboundHandlerAdapter {
        private final Channel inboundChannel;

        private PassthroughProxyBackendHandler(Channel inboundChannel) {
            this.inboundChannel = inboundChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (!inboundChannel.isActive()) {
                PassthroughProxyFrontendHandler.closeOnFlush(ctx.channel());
            }
            else {
                ctx.read();
            }
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    ctx.channel().read();
                }
                else {
                    channelFuture.channel().close();
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            PassthroughProxyFrontendHandler.closeOnFlush(inboundChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.info("exception caught on backend handler", cause);
            PassthroughProxyFrontendHandler.closeOnFlush(ctx.channel());
        }
    }
}
