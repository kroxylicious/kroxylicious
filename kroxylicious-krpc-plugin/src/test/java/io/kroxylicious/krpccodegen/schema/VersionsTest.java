/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionsTest {

    @Test
    void range() {
        Collection<Integer> range = new Versions((short) 3, (short) 5).range();
        assertThat(range).containsExactly(3, 4, 5);
    }
}