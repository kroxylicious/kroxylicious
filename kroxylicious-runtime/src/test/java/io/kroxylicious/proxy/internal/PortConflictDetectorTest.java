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
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), any))),
                argumentSet("any:many exclusive conflicts",
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080, 9081), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9080, 9081), Set.of(), any))),
                argumentSet("any:overlap produces exclusive conflict ",
                        Set.of("exclusive TCP bind of <any>:9081 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of <any>:9081 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080, 9081), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081, 9082), Set.of(), any))),
                argumentSet("loopback:single exclusive conflict",
                        Set.of("exclusive TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("loopback/any:single exclusive conflict",
                        Set.of("exclusive TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), any))),
                argumentSet("any/loopback:single exclusive conflict",
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("any/loopback:single shared conflict",
                        Set.of("shared TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with shared TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'two': shared port cannot bind to different hosts"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), loopback))),
                argumentSet("loopback/any:single shared conflict",
                        Set.of("shared TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'one' conflicts with shared TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': shared port cannot bind to different hosts"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("shared/exclusivity mismatch",
                        Set.of("shared TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), any))),
                argumentSet("exclusivity/shared mismatch",
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with shared TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("shared tls mismatch",
                        Set.of("shared TLS bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with shared TCP bind of <any>:9080 for listener 'default' of virtual cluster 'two': shared port cannot be both TLS and non-TLS"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any, true),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any, false))),
                argumentSet("shared tls mismatch",
                        Set.of("shared TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'one' conflicts with shared TLS bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'two': shared port cannot be both TLS and non-TLS"),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, false),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), loopback, true))),
                argumentSet("different exclusive ports",
                        Set.of(),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), any))),
                argumentSet("same shared ports",
                        Set.of(),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("same shared shared ports different interfaces with differing tls",
                        Set.of(),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, true),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), privateUse, false))),
                argumentSet("same exclusive port different interfaces",
                        Set.of(),
                        null,
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), privateUse),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("different exclusive ports, collides with other exclusive port on any interface",
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with another (non-cluster) exclusive port 0.0.0.0:9080"),
                        new HostPort("0.0.0.0", 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), any))),
                argumentSet("different exclusive ports on any interface, collides with other exclusive port on specific interface",
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with another (non-cluster) exclusive port 127.0.0.1:9080"),
                        new HostPort(loopback, 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), any),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), any))),
                argumentSet("different exclusive ports on specific interface, collides with other exclusive port on any interface",
                        Set.of("exclusive TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'one' conflicts with another (non-cluster) exclusive port 0.0.0.0:9080"),
                        new HostPort("0.0.0.0", 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), loopback),
                                createMockVirtualCluster("two", Set.of(9081), Set.of(), loopback))),
                argumentSet("same shared ports, collides with other exclusive port on any interface",
                        Set.of("shared TCP bind of <any>:9080 for listener 'default' of virtual cluster 'one' conflicts with another (non-cluster) exclusive port 0.0.0.0:9080"),
                        new HostPort("0.0.0.0", 9080),
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), any),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), any))),
                argumentSet("same shared shared ports different interfaces with differing tls, collides with other exclusive port",
                        Set.of("shared TLS bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'one' conflicts with another (non-cluster) exclusive port 127.0.0.1:9080"),
                        new HostPort(loopback, 9080),
                        List.of(createMockVirtualCluster("one", Set.of(), Set.of(9080), loopback, true),
                                createMockVirtualCluster("two", Set.of(), Set.of(9080), privateUse, false))),
                argumentSet("same exclusive port different interfaces, collides with other exclusive port",
                        Set.of("exclusive TCP bind of 127.0.0.1:9080 for listener 'default' of virtual cluster 'two' conflicts with another (non-cluster) exclusive port 127.0.0.1:9080"),
                        new HostPort(loopback, 9080),
                        List.of(createMockVirtualCluster("one", Set.of(9080), Set.of(), privateUse),
                                createMockVirtualCluster("two", Set.of(9080), Set.of(), loopback))),
                argumentSet("any:single conflict within virtual cluster",
                        Set.of("exclusive TCP bind of <any>:9080 for listener 'listener1' of virtual cluster 'one' conflicts with exclusive TCP bind of <any>:9080 for listener 'listener2' of virtual cluster 'one': exclusive port collision"),
                        null,
                        List.of(createMockVirtualCluster("one", getVirtualClusterListenerModel(Set.of(9080), Set.of(), any, false, "listener1"),
                                getVirtualClusterListenerModel(Set.of(9080), Set.of(), any, false, "listener2")))));
    }

    @ParameterizedTest
    @MethodSource
    void portConflict(Set<String> expectedExceptionMessages, HostPort otherExclusivePort, List<VirtualClusterModel> clusters) {
        Optional<HostPort> maybeOtherExclusivePort = Optional.ofNullable(otherExclusivePort);
        if (expectedExceptionMessages.isEmpty()) {
            detector.validate(clusters, maybeOtherExclusivePort);
        }
        else {
            var e = assertThrows(IllegalStateException.class, () -> detector.validate(clusters, maybeOtherExclusivePort));
            for (String expectedMessage : expectedExceptionMessages) {
                assertThat(e).hasStackTraceContaining(expectedMessage);
            }
        }
    }

    private static VirtualClusterModel createMockVirtualCluster(String clusterName, Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress) {
        return createMockVirtualCluster(clusterName, exclusivePorts, sharedPorts, bindAddress, false);
    }

    private static VirtualClusterModel createMockVirtualCluster(String clusterName, Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress,
                                                                boolean tls) {
        return createMockVirtualCluster(clusterName, getVirtualClusterListenerModel(exclusivePorts, sharedPorts, bindAddress, tls, "default"));
    }

    private static VirtualClusterModel createMockVirtualCluster(String clusterName, VirtualClusterListenerModel... listenerModel) {
        var virtualClusterModel = mock(VirtualClusterModel.class);
        var listenerMap = Arrays.stream(listenerModel).collect(Collectors.toMap(VirtualClusterListenerModel::name, l -> l));
        when(virtualClusterModel.listeners()).thenAnswer(invocation -> listenerMap);
        when(virtualClusterModel.getClusterName()).thenReturn(clusterName);
        listenerMap.values().forEach(l -> when(l.virtualCluster()).thenReturn(virtualClusterModel));
        return virtualClusterModel;
    }

    @NonNull
    private static VirtualClusterListenerModel getVirtualClusterListenerModel(Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress, boolean tls,
                                                                              String listenerName) {
        var cluster = mock(VirtualClusterListenerModel.class);
        when(cluster.getExclusivePorts()).thenReturn(exclusivePorts);
        when(cluster.getSharedPorts()).thenReturn(sharedPorts);
        when(cluster.getBindAddress()).thenReturn(Optional.ofNullable(bindAddress));
        when(cluster.isUseTls()).thenReturn(tls);
        when(cluster.name()).thenReturn(listenerName);
        return cluster;
    }

}
