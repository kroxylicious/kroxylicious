/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class GroupResourceTest {
    private static Stream<Arguments> implies() {
        return Stream.of(argumentSet("READ", GroupResource.READ, EnumSet.of(GroupResource.DESCRIBE)),
                argumentSet("ALTER_CONFIGS", GroupResource.ALTER_CONFIGS, EnumSet.of(GroupResource.DESCRIBE_CONFIGS)),
                argumentSet("DESCRIBE", GroupResource.DESCRIBE, EnumSet.noneOf(GroupResource.class)),
                argumentSet("DESCRIBE_CONFIGS", GroupResource.DESCRIBE_CONFIGS, EnumSet.noneOf(GroupResource.class)),
                argumentSet("DELETE", GroupResource.DELETE, EnumSet.of(GroupResource.DESCRIBE)));
    }

    @MethodSource
    @ParameterizedTest
    void implies(GroupResource resource, Set<GroupResource> impliedResources) {
        Set<GroupResource> implies = resource.implies();
        assertThat(implies).isEqualTo(impliedResources);
    }

    @Test
    void allImplicationsTested() {
        Set<Object> collect = implies().collect(Collectors.toSet()).stream().map(arguments -> arguments.get()[0]).collect(Collectors.toSet());
        assertThat(collect).isEqualTo(Set.of(GroupResource.values()));
    }
}