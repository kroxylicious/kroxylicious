/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentReconfigureExceptionTest {

    @Test
    void defaultConstructorProducesInformativeMessage() {
        var ex = new ConcurrentReconfigureException();
        assertThat(ex.getMessage()).contains("reconfigure");
        assertThat(ex.getMessage()).contains("retry");
    }

    @Test
    void customMessageConstructorPropagatesMessage() {
        var ex = new ConcurrentReconfigureException("custom diagnostic");
        assertThat(ex.getMessage()).isEqualTo("custom diagnostic");
    }

    @Test
    void isUncheckedRuntimeException() {
        // Documented contract: thrown via future-exceptional-completion, not synchronously.
        // Verifying the type so callers can rely on instanceof checks in whenComplete.
        assertThat(new ConcurrentReconfigureException()).isInstanceOf(RuntimeException.class);
    }
}
