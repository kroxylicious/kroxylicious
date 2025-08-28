/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.rtsp.RtspHeaderNames.CONTENT_TYPE;

public class RoutingHttpServer extends SimpleChannelInboundHandler<HttpObject> {

    private final Map<String, Function<HttpRequest, HttpResponse>> routes;
    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingHttpServer.class);

    public RoutingHttpServer(Map<String, Function<HttpRequest, HttpResponse>> routes) {
        this.routes = routes;
    }

    public static RoutingHttpServerBuilder builder() {
        return new RoutingHttpServerBuilder();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest req) {
            boolean keepAlive = HttpUtil.isKeepAlive(req);

            HttpResponse response;
            if (HttpMethod.GET.equals(req.method())) {
                response = getResponse(req);
            }
            else {
                response = responseWithStatus(req, METHOD_NOT_ALLOWED);
                // DoS defence - stops the server reading an excessive POST/PUT body to prevent resource allocation,
                ctx.channel().config().setAutoRead(false);
                keepAlive = false;
            }

            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            }
            else {
                // Tell the client we're going to close the connection.
                response.headers().set(CONNECTION, CLOSE);
            }

            ChannelFuture f = ctx.write(response);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    private HttpResponse getResponse(HttpRequest req) {
        if (routes.containsKey(req.uri())) {
            try {
                return routes.get(req.uri()).apply(req);
            }
            catch (Exception e) {
                LOGGER.error("exception while invoking endpoint for route {}", req.uri(), e);
                return responseWithStatus(req, INTERNAL_SERVER_ERROR);
            }
        }
        else {
            return responseWithStatus(req, NOT_FOUND);
        }
    }

    public static FullHttpResponse responseWithStatus(HttpRequest req, HttpResponseStatus status) {
        return responseWithBody(req, status, status.reasonPhrase());
    }

    public static FullHttpResponse responseWithBody(HttpRequest req, HttpResponseStatus status, String content) {
        FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status,
                Unpooled.wrappedBuffer(content.getBytes(StandardCharsets.UTF_8)));
        response.headers()
                .set(CONTENT_TYPE, TEXT_PLAIN)
                .setInt(CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("exception caught in MetricsServer", cause);
        ctx.close();
    }

    public static class RoutingHttpServerBuilder {

        private final Map<String, Function<HttpRequest, HttpResponse>> routes = new HashMap<>();

        RoutingHttpServerBuilder withRoute(String path, Function<HttpRequest, HttpResponse> responseFunction) {
            routes.put(path, responseFunction);
            return this;
        }

        RoutingHttpServer build() {
            return new RoutingHttpServer(routes);
        }

    }
}
