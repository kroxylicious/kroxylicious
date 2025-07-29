/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.net.InetSocketAddress;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointTest {

    @Test
    void shouldCreateEndpointFromChannel() {
        // Given
        Channel ch = new EmbeddedChannel(new NioServerSocketChannel() {
            @Override
            public InetSocketAddress localAddress() {
                return new InetSocketAddress("localhost", 1234);
            }
        }, DefaultChannelId.newInstance(), true, false);

        // When
        Endpoint actual = Endpoint.createEndpoint(ch, false);

        // Then
        assertThat(actual)
                .isNotNull()
                .satisfies(
                        endpoint -> {
                            assertThat(endpoint.bindingAddress()).isPresent().hasValue("127.0.0.1");
                            assertThat(endpoint.port()).isEqualTo(1234);
                        });
    }

    @Test
    void shouldCreateEndpointFromPort() {
        // Given
        int port = 1234;

        // When
        Endpoint actual = Endpoint.createEndpoint(port, false);

        // Then
        assertThat(actual)
                .isNotNull()
                .satisfies(
                        endpoint -> {
                            assertThat(endpoint.bindingAddress()).isEmpty();
                            assertThat(endpoint.port()).isEqualTo(1234);
                        });
    }

    @Test
    void shouldCreateEndpointFromBindingAddress() {
        // Given
        var bindingAddress = "127.0.0.1";

        // When
        Endpoint actual = Endpoint.createEndpoint(Optional.of(bindingAddress), 1234, false);

        // Then
        assertThat(actual)
                .isNotNull()
                .satisfies(
                        endpoint -> {
                            assertThat(endpoint.bindingAddress()).isPresent().hasValue("127.0.0.1");
                            assertThat(endpoint.port()).isEqualTo(1234);
                        });
    }
}
