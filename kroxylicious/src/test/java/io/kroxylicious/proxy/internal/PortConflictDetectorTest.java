/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.model.VirtualCluster;

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
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("any:many exclusive conflicts",
                        createMockVirtualCluster(Set.of(9080, 9081), Set.of(), any),
                        createMockVirtualCluster(Set.of(9080, 9081), Set.of(), any),
                        "The exclusive bind of port(s) 9080,9081 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("any:overlap produces exclusive conflict ",
                        createMockVirtualCluster(Set.of(9080, 9081), Set.of(), any),
                        createMockVirtualCluster(Set.of(9081, 9082), Set.of(), any),
                        "The exclusive bind of port(s) 9081 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("loopback:single exclusive conflict",
                        createMockVirtualCluster(Set.of(9080), Set.of(), loopback),
                        createMockVirtualCluster(Set.of(9080), Set.of(), loopback),
                        "The exclusive bind of port(s) 9080 to 127.0.0.1 would conflict with existing exclusive port bindings on 127.0.0.1."),
                Arguments.of("loopback/any:single exclusive conflict",
                        createMockVirtualCluster(Set.of(9080), Set.of(), loopback),
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on 127.0.0.1."),
                Arguments.of("any/loopback:single exclusive conflict",
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        createMockVirtualCluster(Set.of(9080), Set.of(), loopback),
                        "The exclusive bind of port(s) 9080 to 127.0.0.1 would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("any/loopback:single shared conflict",
                        createMockVirtualCluster(Set.of(), Set.of(9080), any),
                        createMockVirtualCluster(Set.of(), Set.of(9080), loopback),
                        "The shared bind of port(s) 9080 to 127.0.0.1 would conflict with existing shared port bindings on <any>."),
                Arguments.of("loopback/any:single shared conflict",
                        createMockVirtualCluster(Set.of(), Set.of(9080), loopback),
                        createMockVirtualCluster(Set.of(), Set.of(9080), any),
                        "The shared bind of port(s) 9080 to <any> would conflict with existing shared port bindings on 127.0.0.1."),
                Arguments.of("shared/exclusivity mismatch",
                        createMockVirtualCluster(Set.of(), Set.of(9080), any),
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        "The exclusive bind of port(s) 9080 to <any> would conflict with existing shared port bindings on <any>."),
                Arguments.of("exclusivity/shared mismatch",
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        createMockVirtualCluster(Set.of(), Set.of(9080), any),
                        "The shared bind of port(s) 9080 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("shared tls mismatch",
                        createMockVirtualCluster(Set.of(), Set.of(9080), any, true),
                        createMockVirtualCluster(Set.of(), Set.of(9080), any, false),
                        "The shared bind of port 9080 to <any> has conflicting TLS settings with existing port on the same interface."),
                Arguments.of("shared tls mismatch",
                        createMockVirtualCluster(Set.of(), Set.of(9080), loopback, false),
                        createMockVirtualCluster(Set.of(), Set.of(9080), loopback, true),
                        "The shared bind of port 9080 to 127.0.0.1 has conflicting TLS settings with existing port on the same interface."),
                Arguments.of("different exclusive ports",
                        createMockVirtualCluster(Set.of(9080), Set.of(), any),
                        createMockVirtualCluster(Set.of(9081), Set.of(), any),
                        null),
                Arguments.of("same shared ports",
                        createMockVirtualCluster(Set.of(), Set.of(9080), any),
                        createMockVirtualCluster(Set.of(), Set.of(9080), any),
                        null),
                Arguments.of("same shared shared ports different interfaces with differing tls",
                        createMockVirtualCluster(Set.of(), Set.of(9080), loopback, true),
                        createMockVirtualCluster(Set.of(), Set.of(9080), privateUse, false),
                        null),
                Arguments.of("same exclusive port different interfaces",
                        createMockVirtualCluster(Set.of(9080), Set.of(), privateUse),
                        createMockVirtualCluster(Set.of(9080), Set.of(), loopback),
                        null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void portConflict(String name, VirtualCluster virtualCluster1, VirtualCluster virtualCluster2, String expectedMessageSuffix) {
        var clusters = Map.of("one", virtualCluster1, "two", virtualCluster2);
        if (expectedMessageSuffix == null) {
            detector.validate(clusters);
        }
        else {
            var e = assertThrows(IllegalStateException.class, () -> detector.validate(clusters));
            assertThat(e).hasStackTraceContaining(expectedMessageSuffix);
        }
    }

    private static VirtualCluster createMockVirtualCluster(Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress) {
        return createMockVirtualCluster(exclusivePorts, sharedPorts, bindAddress, false);
    }

    private static VirtualCluster createMockVirtualCluster(Set<Integer> exclusivePorts, Set<Integer> sharedPorts, String bindAddress, boolean tls) {
        VirtualCluster cluster = mock(VirtualCluster.class);
        when(cluster.getExclusivePorts()).thenReturn(exclusivePorts);
        when(cluster.getSharedPorts()).thenReturn(sharedPorts);
        when(cluster.getBindAddress()).thenReturn(Optional.ofNullable(bindAddress));
        when(cluster.isUseTls()).thenReturn(tls);
        return cluster;
    }

}