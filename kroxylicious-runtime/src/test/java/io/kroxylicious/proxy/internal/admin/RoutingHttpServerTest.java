/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.admin;

import java.net.URI;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import static io.kroxylicious.proxy.internal.admin.RoutingHttpServer.builder;
import static io.kroxylicious.proxy.internal.admin.RoutingHttpServer.responseWithBody;
import static io.kroxylicious.proxy.internal.admin.RoutingHttpServer.responseWithStatus;
import static org.assertj.core.api.Assertions.assertThat;

class RoutingHttpServerTest {

    @Test
    void shouldRouteRequestForKnownPath() {
        // Given

        var route = URI.create("https://localhost/mypath").toString();
        var handler = builder().withRoute(route, rq -> responseWithStatus(rq, HttpResponseStatus.OK)).build();
        var channel = new EmbeddedChannel(handler);
        var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, route);

        // When
        channel.writeInbound(request);

        // Then
        var response = channel.readOutbound();
        assertThat(response)
                .isInstanceOfSatisfying(HttpResponse.class, r -> assertThat(r.status()).isEqualTo(HttpResponseStatus.OK));
    }

    @Test
    void shouldSupportResponseWithBody() {
        // Given

        var route = URI.create("https://localhost/mypath").toString();
        var handler = builder().withRoute(route, rq -> responseWithBody(rq, HttpResponseStatus.OK, "Hello World")).build();
        var channel = new EmbeddedChannel(handler);
        var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, route);

        // When
        channel.writeInbound(request);

        // Then
        assertThat(channel.<Object> readOutbound())
                .isInstanceOfSatisfying(FullHttpResponse.class, r -> {
                    assertThat(r.status()).isEqualTo(HttpResponseStatus.OK);
                    assertThat(r.content())
                            .extracting(ByteBufUtil::getBytes)
                            .extracting(String::new)
                            .isEqualTo("Hello World");
                });
    }

    @Test
    void shouldRespond404ForUnknownPath() {
        // Given

        var route = URI.create("https://localhost/mypath").toString();
        var another = URI.create("https://localhost/another").toString();
        var handler = builder().withRoute(route, rq -> responseWithStatus(rq, HttpResponseStatus.OK)).build();
        var channel = new EmbeddedChannel(handler);
        var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, another);

        // When
        channel.writeInbound(request);

        // Then
        var response = channel.readOutbound();
        assertThat(response)
                .isInstanceOfSatisfying(HttpResponse.class, r -> assertThat(r.status()).isEqualTo(HttpResponseStatus.NOT_FOUND));
    }

    @Test
    void shouldRejectUnsupportedMethodAndCloseConnection() {
        // Given

        var route = URI.create("https://localhost/mypath").toString();
        var handler = builder().withRoute(route, rq -> responseWithStatus(rq, HttpResponseStatus.OK)).build();
        var channel = new EmbeddedChannel(handler);
        var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, route);

        // When
        channel.writeInbound(request);

        // Then
        var response = channel.readOutbound();
        assertThat(response)
                .isInstanceOfSatisfying(HttpResponse.class, r -> assertThat(r.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED));
        assertThat(channel.isOpen()).isFalse();
    }
}