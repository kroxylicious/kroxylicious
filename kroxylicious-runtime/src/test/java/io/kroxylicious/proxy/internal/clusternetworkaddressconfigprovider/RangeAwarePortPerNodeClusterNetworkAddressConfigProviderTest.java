/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.IntRangeSpec;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.NamedRangeSpec;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeAwarePortPerNodeClusterNetworkAddressConfigProviderTest {

    public static final String BOOTSTRAP_HOST = "cluster.kafka.example.com";
    private static final String BOOTSTRAP = BOOTSTRAP_HOST + ":1235";
    private static final HostPort BOOSTRAP_HOSTPORT = HostPort.parse(BOOTSTRAP);

    @Test
    void rangesMustBeNonEmpty() {
        List<NamedRangeSpec> empty = List.of();
        assertThatThrownBy(() -> getConfig(empty))
                                                  .isInstanceOf(IllegalArgumentException.class)
                                                  .hasMessage("node id ranges empty");
    }

    @Test
    void brokerAddressSingleRange() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2))))
        );
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
    }

    @Test
    void brokerAddressPortsInferredFromBootstrapIfNotExplicitlySupplied() {
        List<NamedRangeSpec> namedRangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        "broker$(nodeId).kafka.example.com",
                        null,
                        namedRangeSpecs
                )
        );
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
    }

    @Test
    void brokerAddressInferredFromBootstrapIfNotExplicitlySupplied() {
        List<NamedRangeSpec> namedRangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        null,
                        null,
                        namedRangeSpecs
                )
        );
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1237));
    }

    @Test
    void nodeAddressPatternCannotBeBlank() {
        List<NamedRangeSpec> rangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        assertThatThrownBy(
                () -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        "",
                        null,
                        rangeSpecs
                )
        )
         .isInstanceOf(IllegalArgumentException.class)
         .hasMessage("nodeAddressPattern cannot be blank");
    }

    @Test
    void nodeAddressPatternCannotContainUnexpectedReplacementPatterns() {
        List<NamedRangeSpec> rangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        assertThatThrownBy(
                () -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        "node-$(typoedNodeId).broker.com",
                        null,
                        rangeSpecs
                )
        )
         .isInstanceOf(IllegalArgumentException.class)
         .hasMessage("nodeAddressPattern contains an unexpected replacement token '$(typoedNodeId)'");
    }

    @Test
    void nodeAddressPatternCannotContainPort() {
        List<NamedRangeSpec> rangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        assertThatThrownBy(
                () -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        "localhost:8080",
                        null,
                        rangeSpecs
                )
        )
         .isInstanceOf(IllegalArgumentException.class)
         .hasMessage("nodeAddressPattern cannot have port specifier.  Found port : 8080 within localhost:8080");
    }

    @Test
    void nodeStartPortCannotBeLessThanOne() {
        List<NamedRangeSpec> rangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        assertThatThrownBy(
                () -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        "localhost",
                        0,
                        rangeSpecs
                )
        )
         .isInstanceOf(IllegalArgumentException.class)
         .hasMessage("nodeStartPort cannot be less than 1");
    }

    @Test
    void nodePortRangeCannotCollideWithBootstrapPort() {
        List<NamedRangeSpec> rangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 3)));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP_HOST + ":1235");
        assertThatThrownBy(
                () -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        bootstrapAddress,
                        "localhost",
                        // node id 1 will be assigned port 1235 and collide with bootstrap
                        1234,
                        rangeSpecs
                )
        )
         .isInstanceOf(IllegalArgumentException.class)
         .hasMessage("the port used by the bootstrap address (1235) collides with the node id range: brokers:[0,3) mapped to ports [1234,1237)");
    }

    @Test
    void getClusterBootstrap() {
        List<NamedRangeSpec> namedRangeSpecs = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)));
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        BOOSTRAP_HOSTPORT,
                        "broker$(nodeId).kafka.example.com",
                        1236,
                        namedRangeSpecs
                )
        );
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(BOOSTRAP_HOSTPORT);
    }

    @Test
    void exclusivePortsSingleRange() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2))))
        );
        assertThat(provider.getExclusivePorts()).containsExactly(1235, 1236, 1237);
    }

    @Test
    void discoveryAddressMapSingleRange() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2))))
        );

        Map<Integer, HostPort> expected = Map.of(
                0,
                new HostPort("broker0.kafka.example.com", 1236),
                1,
                new HostPort("broker1.kafka.example.com", 1237)
        );
        assertThat(provider.discoveryAddressMap()).isEqualTo(expected);
    }

    @Test
    void brokerAddressMultipleRanges() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)), new NamedRangeSpec("controllers", new IntRangeSpec(3, 5))))
        );
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
        assertThat(provider.getBrokerAddress(3)).isEqualTo(new HostPort("broker3.kafka.example.com", 1238));
        assertThat(provider.getBrokerAddress(4)).isEqualTo(new HostPort("broker4.kafka.example.com", 1239));
    }

    @Test
    void brokerAddressUnknownNodeId() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)), new NamedRangeSpec("controllers", new IntRangeSpec(3, 5))))
        );
        String expectedMessage = "Cannot generate node address for node id 5 as it is not contained in the ranges defined for provider with downstream bootstrap "
                                 + BOOTSTRAP;
        assertThatThrownBy(() -> provider.getBrokerAddress(5)).isInstanceOf(IllegalArgumentException.class).hasMessage(expectedMessage);
    }

    @Test
    void discoveryAddressMapMultipleRanges() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)), new NamedRangeSpec("controllers", new IntRangeSpec(3, 5))))
        );

        Map<Integer, HostPort> expected = Map.of(
                0,
                new HostPort("broker0.kafka.example.com", 1236),
                1,
                new HostPort("broker1.kafka.example.com", 1237),
                3,
                new HostPort("broker3.kafka.example.com", 1238),
                4,
                new HostPort("broker4.kafka.example.com", 1239)
        );
        assertThat(provider.discoveryAddressMap()).isEqualTo(expected);
    }

    static Stream<Arguments> overlappingNodeIdRangesAreInvalid() {
        Arguments twoRangesWithOverlap = Arguments.arguments(
                List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)), new NamedRangeSpec("controllers", new IntRangeSpec(1, 2))),
                "some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): 'brokers:[0,2)' collides with 'controllers:[1,2)'"
        );
        Arguments threeRangesWithFirstAndLastOverlap = Arguments.arguments(
                List.of(
                        new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)),
                        new NamedRangeSpec("controllers", new IntRangeSpec(7, 8)),
                        new NamedRangeSpec("other", new IntRangeSpec(1, 2))
                ),
                "some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): 'brokers:[0,2)' collides with 'other:[1,2)'"
        );
        Arguments multipleOverlaps = Arguments.arguments(
                List.of(
                        new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)),
                        new NamedRangeSpec("controllers", new IntRangeSpec(1, 4)),
                        new NamedRangeSpec("other", new IntRangeSpec(3, 5))
                ),
                "some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): 'brokers:[0,2)' collides with 'controllers:[1,4)', 'controllers:[1,4)' collides with 'other:[3,5)'"
        );
        return Stream.of(twoRangesWithOverlap, threeRangesWithFirstAndLastOverlap, multipleOverlaps);
    }

    @MethodSource
    @ParameterizedTest
    void overlappingNodeIdRangesAreInvalid(List<NamedRangeSpec> namedRangeSpecs, String expectedException) {
        assertThatThrownBy(() -> getConfig(namedRangeSpecs))
                                                            .isInstanceOf(IllegalArgumentException.class)
                                                            .hasMessage(expectedException);
    }

    @Test
    void rangeMustBeNonEmpty() {
        List<NamedRangeSpec> emptyRange = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 0)));
        assertThatThrownBy(() -> getConfig(emptyRange))
                                                       .isInstanceOf(IllegalArgumentException.class)
                                                       .hasMessage("invalid nodeIdRange: brokers, end of range: 0 (exclusive) is before start of range: 0 (inclusive)");
    }

    @Test
    void rangesMustHaveUniqueNames() {
        List<NamedRangeSpec> emptyRange = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 1)), new NamedRangeSpec("brokers", new IntRangeSpec(1, 2)));
        assertThatThrownBy(() -> getConfig(emptyRange))
                                                       .isInstanceOf(IllegalArgumentException.class)
                                                       .hasMessage("non-unique nodeIdRange names discovered: [brokers]");
    }

    @Test
    void exclusivePortsMultipleRanges() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 2)), new NamedRangeSpec("controllers", new IntRangeSpec(3, 5))))
        );
        assertThat(provider.getExclusivePorts()).containsExactly(1235, 1236, 1237, 1238, 1239);
    }

    @Test
    void allNodeIdsMustBeMappableToAValidPort() {
        List<NamedRangeSpec> ranges = List.of(new NamedRangeSpec("brokers", new IntRangeSpec(0, 65535)));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP_HOST + ":1");
        assertThatThrownBy(() -> {
            getConfig(ranges, 2, bootstrapAddress);
        })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("The maximum port mapped exceeded 65535");
    }

    private static @NonNull RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig getConfig(List<NamedRangeSpec> namedRangeSpecs) {
        return getConfig(namedRangeSpecs, 1236, BOOSTRAP_HOSTPORT);
    }

    private static @NonNull RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig getConfig(
            List<NamedRangeSpec> namedRangeSpecs,
            int nodeStartPort,
            HostPort bootstrapAddress
    ) {
        return new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                bootstrapAddress,
                "broker$(nodeId).kafka.example.com",
                nodeStartPort,
                namedRangeSpecs
        );
    }

}
