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

import static io.kroxylicious.filter.authorization.TransactionalIdResource.DESCRIBE;
import static io.kroxylicious.filter.authorization.TransactionalIdResource.TWO_PHASE_COMMIT;
import static io.kroxylicious.filter.authorization.TransactionalIdResource.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TransactionalIdResourceTest {

    private static Stream<Arguments> implies() {
        return Stream.of(argumentSet("WRITE", WRITE, EnumSet.of(DESCRIBE)),
                argumentSet("DESCRIBE", DESCRIBE, EnumSet.noneOf(TransactionalIdResource.class)),
                argumentSet("TWO_PHASE_COMMIT", TWO_PHASE_COMMIT, EnumSet.noneOf(TransactionalIdResource.class)));
    }

    @MethodSource
    @ParameterizedTest
    void implies(TransactionalIdResource resource, Set<TransactionalIdResource> impliedResources) {
        Set<TransactionalIdResource> implies = resource.implies();
        assertThat(implies).isEqualTo(impliedResources);
    }

    @Test
    void allImplicationsTested() {
        Set<Object> collect = implies().collect(Collectors.toSet()).stream().map(arguments -> arguments.get()[0]).collect(Collectors.toSet());
        assertThat(collect).isEqualTo(Set.of(TransactionalIdResource.values()));
    }

}