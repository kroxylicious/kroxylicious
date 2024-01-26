/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.net;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class TlsServer {

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final SslContext context;
    private Channel channel;

    public TlsServer(SslContext context) {
        this.context = context;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
    }

    public int start() {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(context.newHandler(ch.alloc()));
                            p.addLast(new HttpServerCodec());
                            p.addLast(new Respond200AndCloseHandler());
                        }
                    });

            channel = b.bind("0.0.0.0", 0).sync().channel();
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        try {
            channel.closeFuture().get(5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class Respond200AndCloseHandler extends SimpleChannelInboundHandler<HttpObject> {

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpRequest req) {
                HttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK);
                response.headers().set(CONNECTION, CLOSE);
                ChannelFuture f = ctx.write(response);
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
