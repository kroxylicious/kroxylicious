/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OnFailureTest {

    @Test
    void shouldHaveThreeValues() {
        assertThat(OnFailure.values()).containsExactly(OnFailure.ROLLBACK, OnFailure.TERMINATE, OnFailure.CONTINUE);
    }

    @Test
    void shouldResolveFromName() {
        assertThat(OnFailure.valueOf("ROLLBACK")).isEqualTo(OnFailure.ROLLBACK);
        assertThat(OnFailure.valueOf("TERMINATE")).isEqualTo(OnFailure.TERMINATE);
        assertThat(OnFailure.valueOf("CONTINUE")).isEqualTo(OnFailure.CONTINUE);
    }
}
