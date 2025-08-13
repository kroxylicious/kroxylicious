/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedBootstrapSelectionStrategyTest {

    @Test
    void shouldRejectNegativeValuesForChoice() {
        assertThatThrownBy(() -> new FixedBootstrapSelectionStrategy(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}