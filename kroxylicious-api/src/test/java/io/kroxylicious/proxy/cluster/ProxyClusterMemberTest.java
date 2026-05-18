/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.cluster;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProxyClusterMemberTest {

    private static final ProxyClusterEndpoint TCP_ENDPOINT = new ProxyClusterEndpoint("localhost", 9092, Transport.TCP);

    @Test
    void shouldCreateValidMember() {
        var member = new ProxyClusterMember(1, null, List.of(TCP_ENDPOINT));

        assertThat(member.nodeId()).isEqualTo(1);
        assertThat(member.rackId()).isNull();
        assertThat(member.endpoints()).containsExactly(TCP_ENDPOINT);
    }

    @Test
    void shouldAcceptRackId() {
        var member = new ProxyClusterMember(1, "rack-a", List.of(TCP_ENDPOINT));

        assertThat(member.rackId()).isEqualTo("rack-a");
    }

    @Test
    void shouldRejectNonPositiveNodeId() {
        assertThatThrownBy(() -> new ProxyClusterMember(0, null, List.of(TCP_ENDPOINT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nodeId must be positive");

        assertThatThrownBy(() -> new ProxyClusterMember(-1, null, List.of(TCP_ENDPOINT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nodeId must be positive");
    }

    @Test
    void shouldRejectNullEndpoints() {
        assertThatThrownBy(() -> new ProxyClusterMember(1, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectEmptyEndpoints() {
        assertThatThrownBy(() -> new ProxyClusterMember(1, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoints must not be empty");
    }

    @Test
    void shouldDefensivelyCopyEndpoints() {
        var mutable = new java.util.ArrayList<>(List.of(TCP_ENDPOINT));
        var member = new ProxyClusterMember(1, null, mutable);

        mutable.add(new ProxyClusterEndpoint("other", 9093, Transport.TLS));

        assertThat(member.endpoints()).hasSize(1);
    }

    @Test
    void shouldReturnUnmodifiableEndpoints() {
        var member = new ProxyClusterMember(1, null, List.of(TCP_ENDPOINT));

        assertThatThrownBy(() -> member.endpoints().add(TCP_ENDPOINT))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldRejectBlankHost() {
        assertThatThrownBy(() -> new ProxyClusterEndpoint("", 9092, Transport.TCP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be blank");

        assertThatThrownBy(() -> new ProxyClusterEndpoint("  ", 9092, Transport.TCP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be blank");
    }

    @Test
    void shouldRejectNullHost() {
        assertThatThrownBy(() -> new ProxyClusterEndpoint(null, 9092, Transport.TCP))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectInvalidPort() {
        assertThatThrownBy(() -> new ProxyClusterEndpoint("localhost", 0, Transport.TCP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port must be between 1 and 65535");

        assertThatThrownBy(() -> new ProxyClusterEndpoint("localhost", 65536, Transport.TCP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port must be between 1 and 65535");
    }

    @Test
    void shouldRejectNullTransport() {
        assertThatThrownBy(() -> new ProxyClusterEndpoint("localhost", 9092, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldAcceptValidPorts() {
        assertThat(new ProxyClusterEndpoint("localhost", 1, Transport.TCP).port()).isEqualTo(1);
        assertThat(new ProxyClusterEndpoint("localhost", 65535, Transport.TCP).port()).isEqualTo(65535);
    }
}
