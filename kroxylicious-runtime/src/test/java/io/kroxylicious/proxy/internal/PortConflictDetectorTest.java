/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PortConflictDetectorTest {

    private final PortConflictDetector detector = new PortConflictDetector();

    public static Stream<Arguments> portConflict() {
        String any = null;
        var loopback = "127.0.0.1";
        var privateUse = "192.168.0.1";
        return Stream.of(
                Arguments.of("any:single conflict",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), any),
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null),
                Arguments.of("any:many exclusive conflicts",
                        createMockVirtualCluster("one", Set.of(9080, 9081), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9080, 9081), Set.of(), any),
                        "The exclusive bind of port(s) 9080,9081 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null),
                Arguments.of("any:overlap produces exclusive conflict ",
                        createMockVirtualCluster("one", Set.of(9080, 9081), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9081, 9082), Set.of(), any),
                        "The exclusive bind of port(s) 9081 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null),
                Arguments.of("loopback:single exclusive conflict",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback),
                        "The exclusive bind of port(s) 9080 to 127.0.0.1 would conflict with existing exclusive port bindings on 127.0.0.1.",
                        null),
                Arguments.of("loopback/any:single exclusive conflict",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), any),
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on 127.0.0.1.",
                        null),
                Arguments.of("any/loopback:single exclusive conflict",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback),
                        "The exclusive bind of port(s) 9080 to 127.0.0.1 would conflict with existing exclusive port bindings on <any>.",
                        null),
                Arguments.of("any/loopback:single shared conflict",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), loopback),
                        "The shared bind of port(s) 9080 to 127.0.0.1 would conflict with existing shared port bindings on <any>.",
                        null),
                Arguments.of("loopback/any:single shared conflict",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), any),
                        "The shared bind of port(s) 9080 to <any> would conflict with existing shared port bindings on 127.0.0.1.",
                        null),
                Arguments.of("shared/exclusivity mismatch",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), any),
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing shared port bindings on <any>.",
                        null),
                Arguments.of("exclusivity/shared mismatch",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                        "The shared bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null),
                Arguments.of("shared tls mismatch",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), any, true),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), any, false),
                        "The shared bind of port 9080 to <any> has conflicting TLS settings with existing port on the same interface.",
                        null),
                Arguments.of("shared tls mismatch",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, false),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), loopback, true),
                        "The shared bind of port 9080 to 127.0.0.1 has conflicting TLS settings with existing port on the same interface.",
                        null),
                Arguments.of("different exclusive ports",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9081), Set.of(), any),
                        null,
                        null),
                Arguments.of("same shared ports",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), any),
                        null,
                        null),
                Arguments.of("same shared shared ports different interfaces with differing tls",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, true),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), privateUse, false),
                        null,
                        null),
                Arguments.of("same exclusive port different interfaces",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), privateUse),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback),
                        null,
                        null),
                Arguments.of("different exclusive ports, collides with other exclusive port on any interface",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9081), Set.of(), any),
                        "The exclusive bind of port(s) 9080 for virtual cluster 'one' to <any> would conflict with another (non-cluster) port binding",
                        new HostPort("0.0.0.0", 9080)),
                Arguments.of("different exclusive ports on any interface, collides with other exclusive port on specific interface",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                        createMockVirtualCluster("two", Set.of(9081), Set.of(), any),
                        "The exclusive bind of port(s) 9080 for virtual cluster 'one' to <any> would conflict with another (non-cluster) port binding",
                        new HostPort(loopback, 9080)),
                Arguments.of("different exclusive ports on specific interface, collides with other exclusive port on any interface",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                        createMockVirtualCluster("two", Set.of(9081), Set.of(), loopback),
                        "The exclusive bind of port(s) 9080 for virtual cluster 'one' to 127.0.0.1 would conflict with another (non-cluster) port binding",
                        new HostPort("0.0.0.0", 9080)),
                Arguments.of("same shared ports, collides with other exclusive port on any interface",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), any),
                        "The shared bind of port(s) 9080 for virtual cluster 'one' to <any> would conflict with another (non-cluster) port binding",
                        new HostPort("0.0.0.0", 9080)),
                Arguments.of("same shared shared ports different interfaces with differing tls, collides with other exclusive port",
                        createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, true),
                        createMockVirtualCluster("two", Set.of(), Set.of(9080), privateUse, false),
                        "The shared bind of port(s) 9080 for virtual cluster 'one' to 127.0.0.1 would conflict with another (non-cluster) port binding",
                        new HostPort(loopback, 9080)),
                Arguments.of("same exclusive port different interfaces, collides with other exclusive port",
                        createMockVirtualCluster("one", Set.of(9080), Set.of(), privateUse),
                        createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback),
                        "The exclusive bind of port(s) 9080 for virtual cluster 'two' to 127.0.0.1 would conflict with another (non-cluster) port binding",
                        new HostPort(loopback, 9080)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void portConflict(String name, VirtualClusterModel virtualClusterModel1, VirtualClusterModel virtualClusterModel2, String expectedMessageSuffix,
                      HostPort otherExclusivePort) {
        var clusters = List.of(virtualClusterModel1, virtualClusterModel2);
        Optional<HostPort> maybeOtherExclusivePort = Optional.ofNullable(otherExclusivePort);
        if (expectedMessageSuffix == null) {
            detector.validate(clusters, maybeOtherExclusivePort);
        }
        else {
            var e = assertThrows(IllegalStateException.class, () -> detector.validate(clusters, maybeOtherExclusivePort));
            assertThat(e).hasStackTraceContaining(expectedMessageSuffix);
        }
    }

    private static VirtualClusterModel createMockVirtualCluster(String clusterName, Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress) {
        return createMockVirtualCluster(clusterName, exclusivePorts, sharedPorts, bindAddress, false);
    }

    private static VirtualClusterModel createMockVirtualCluster(String clusterName, Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress,
                                                                boolean tls) {
        VirtualClusterModel cluster = mock(VirtualClusterModel.class);
        when(cluster.getClusterName()).thenReturn(clusterName);
        when(cluster.getExclusivePorts()).thenReturn(exclusivePorts);
        when(cluster.getSharedPorts()).thenReturn(sharedPorts);
        when(cluster.getBindAddress()).thenReturn(Optional.ofNullable(bindAddress));
        when(cluster.isUseTls()).thenReturn(tls);
        return cluster;
    }

}
