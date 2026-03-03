/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.config.ConfigResource.Type.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigResourcesTest {

    // simulates a Future ConfigResource type unknown to proxy
    public static final byte FUTURE_CONFIG_RESOURCE_TYPE = (byte) 256;

    public static Stream<Arguments> filter() {
        Stream<Byte> allKnownValues = Arrays.stream(ConfigResource.Type.values()).map(ConfigResource.Type::id);
        Stream<Byte> unknownValue = Stream.of(FUTURE_CONFIG_RESOURCE_TYPE);
        List<Byte> allValues = Stream.concat(allKnownValues, unknownValue).toList();
        Stream<Arguments> filterKnownTypes = Arrays.stream(ConfigResource.Type.values()).filter(type -> type != UNKNOWN)
                .map(type -> Arguments.argumentSet("filter " + type, allValues, type, List.of(type.id())));
        Stream<Arguments> otherArguments = Stream.of(Arguments.argumentSet("filter UNKNOWN", allValues, UNKNOWN, List.of(UNKNOWN.id(), FUTURE_CONFIG_RESOURCE_TYPE)),
                Arguments.argumentSet("filter empty list", List.of(), TOPIC, List.of()));
        return Stream.concat(filterKnownTypes, otherArguments);
    }

    @MethodSource
    @ParameterizedTest
    void filter(List<Byte> input, ConfigResource.Type resource, List<Byte> expectedOutput) {
        Stream<Byte> filtered = ConfigResources.filter(input.stream(), Function.identity(), resource);
        assertThat(filtered).containsExactlyElementsOf(expectedOutput);
    }

}