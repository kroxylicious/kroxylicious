/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.model.VirtualClusterModel.VirtualClusterListenerModel;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
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
                argumentSet("any:single conflict",
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), any))),
                argumentSet("any:many exclusive conflicts",
                        "The exclusive bind of port(s) 9080,9081 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080, 9081), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9080, 9081), Set.of(), any))),
                argumentSet("any:overlap produces exclusive conflict ",
                        "The exclusive bind of port(s) 9081 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080, 9081), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081, 9082), Set.of(), any))),
                argumentSet("loopback:single exclusive conflict",
                        "The exclusive bind of port(s) 9080 to 127.0.0.1 would conflict with existing exclusive port bindings on 127.0.0.1.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("loopback/any:single exclusive conflict",
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on 127.0.0.1.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), any))),
                argumentSet("any/loopback:single exclusive conflict",
                        "The exclusive bind of port(s) 9080 to 127.0.0.1 would conflict with existing exclusive port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("any/loopback:single shared conflict",
                        "The shared bind of port(s) 9080 to 127.0.0.1 would conflict with existing shared port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), loopback))),
                argumentSet("loopback/any:single shared conflict",
                        "The shared bind of port(s) 9080 to <any> would conflict with existing shared port bindings on 127.0.0.1.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("shared/exclusivity mismatch",
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing shared port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), any))),
                argumentSet("exclusivity/shared mismatch",
                        "The shared bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("one", Set.of(), Set.of(9080), any))),
                argumentSet("shared tls mismatch",
                        "The shared bind of port 9080 to <any> has conflicting TLS settings with existing port on the same interface.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any, true),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any, false))),
                argumentSet("shared tls mismatch",
                        "The shared bind of port 9080 to 127.0.0.1 has conflicting TLS settings with existing port on the same interface.",
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, false),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), loopback, true))),
                argumentSet("different exclusive ports",
                        null,
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), any))),
                argumentSet("same shared ports",
                        null,
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("same shared shared ports different interfaces with differing tls",
                        null,
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, true),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), privateUse, false))),
                argumentSet("same exclusive port different interfaces",
                        null,
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), privateUse),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("different exclusive ports, collides with other exclusive port on any interface",
                        "The exclusive bind of port(s) 9080 for virtual cluster 'one' to <any> would conflict with another (non-cluster) port binding",
                        new HostPort("0.0.0.0", 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), any))),
                argumentSet("different exclusive ports on any interface, collides with other exclusive port on specific interface",
                        "The exclusive bind of port(s) 9080 for virtual cluster 'one' to <any> would conflict with another (non-cluster) port binding",
                        new HostPort(loopback, 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), any))),
                argumentSet("different exclusive ports on specific interface, collides with other exclusive port on any interface",
                        "The exclusive bind of port(s) 9080 for virtual cluster 'one' to 127.0.0.1 would conflict with another (non-cluster) port binding",
                        new HostPort("0.0.0.0", 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), loopback))),
                argumentSet("same shared ports, collides with other exclusive port on any interface",
                        "The shared bind of port(s) 9080 for virtual cluster 'one' to <any> would conflict with another (non-cluster) port binding",
                        new HostPort("0.0.0.0", 9080),
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("same shared shared ports different interfaces with differing tls, collides with other exclusive port",
                        "The shared bind of port(s) 9080 for virtual cluster 'one' to 127.0.0.1 would conflict with another (non-cluster) port binding",
                        new HostPort(loopback, 9080),
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, true),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), privateUse, false))),
                argumentSet("same exclusive port different interfaces, collides with other exclusive port",
                        "The exclusive bind of port(s) 9080 for virtual cluster 'two' to 127.0.0.1 would conflict with another (non-cluster) port binding",
                        new HostPort(loopback, 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), privateUse),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("any:single conflict within virtual cluster",
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>.",
                        null,
                        List.of(createMockVirtualCluster("one", getVirtualClusterListenerModel(Set.of(9080), Set.of(), any, false),
                                getVirtualClusterListenerModel(Set.of(9080), Set.of(), any, false)))));
    }

    @ParameterizedTest
    @MethodSource
    void portConflict(String expectedMessageSuffix, HostPort otherExclusivePort, List<VirtualClusterModel> clusters) {
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
        return createMockVirtualCluster(clusterName, getVirtualClusterListenerModel(exclusivePorts, sharedPorts, bindAddress, tls));
    }

    private static VirtualClusterModel createMockVirtualCluster(String clusterName, VirtualClusterListenerModel... listenerModel) {
        var virtualClusterModel = mock(VirtualClusterModel.class);
        var listenerMap = Arrays.stream(listenerModel).collect(Collectors.toMap(l -> "listener-" + UUID.randomUUID(), l -> l));
        when(virtualClusterModel.listeners()).thenAnswer(invocation -> listenerMap);
        when(virtualClusterModel.getClusterName()).thenReturn(clusterName);
        return virtualClusterModel;
    }

    @NonNull
    private static VirtualClusterListenerModel getVirtualClusterListenerModel(Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress, boolean tls) {
        var cluster = mock(VirtualClusterListenerModel.class);
        when(cluster.getExclusivePorts()).thenReturn(exclusivePorts);
        when(cluster.getSharedPorts()).thenReturn(sharedPorts);
        when(cluster.getBindAddress()).thenReturn(Optional.ofNullable(bindAddress));
        when(cluster.isUseTls()).thenReturn(tls);
        return cluster;
    }

}
