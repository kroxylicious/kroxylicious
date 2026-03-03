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

class TopicResourceTest {
    private static Stream<Arguments> implies() {
        return Stream.of(argumentSet("READ", TopicResource.READ, EnumSet.of(TopicResource.DESCRIBE)),
                argumentSet("WRITE", TopicResource.WRITE, EnumSet.of(TopicResource.DESCRIBE)),
                argumentSet("ALTER", TopicResource.ALTER, EnumSet.of(TopicResource.DESCRIBE)),
                argumentSet("ALTER_CONFIGS", TopicResource.ALTER_CONFIGS, EnumSet.of(TopicResource.DESCRIBE_CONFIGS)),
                argumentSet("CREATE", TopicResource.CREATE, EnumSet.noneOf(TopicResource.class)),
                argumentSet("DESCRIBE", TopicResource.DESCRIBE, EnumSet.noneOf(TopicResource.class)),
                argumentSet("DESCRIBE_CONFIGS", TopicResource.DESCRIBE_CONFIGS, EnumSet.noneOf(TopicResource.class)),
                argumentSet("DELETE", TopicResource.DELETE, EnumSet.of(TopicResource.DESCRIBE)));
    }

    @MethodSource
    @ParameterizedTest
    void implies(TopicResource resource, Set<TopicResource> impliedResources) {
        Set<TopicResource> implies = resource.implies();
        assertThat(implies).isEqualTo(impliedResources);
    }

    @Test
    void allImplicationsTested() {
        Set<Object> collect = implies().collect(Collectors.toSet()).stream().map(arguments -> arguments.get()[0]).collect(Collectors.toSet());
        assertThat(collect).isEqualTo(Set.of(TopicResource.values()));
    }
}