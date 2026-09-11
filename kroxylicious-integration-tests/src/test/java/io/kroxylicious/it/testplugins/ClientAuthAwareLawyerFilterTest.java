/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import org.junit.jupiter.api.Test;

import static io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter.falseValue;
import static io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter.trueValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ClientAuthAwareLawyerFilter defensive copy accessor methods.
 */
class ClientAuthAwareLawyerFilterTest {

    @Test
    void trueValueReturnsDefensiveCopy() {
        byte[] first = trueValue();
        assertThat(first).isNotNull();
        assertThat(first).hasSize(1);
        assertThat(first[0]).isEqualTo((byte) 1);
        assertThat(first).isEqualTo(new byte[]{ 1 });

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0;

        byte[] second = trueValue();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void falseValueReturnsDefensiveCopy() {
        byte[] first = falseValue();
        assertThat(first).isNotNull();
        assertThat(first).hasSize(1);
        assertThat(first[0]).isEqualTo((byte) 0);
        assertThat(first).isEqualTo(new byte[]{ 0 });

        byte[] originalCopy = first.clone();
        first[0] = (byte) 1;

        byte[] second = falseValue();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }
}
