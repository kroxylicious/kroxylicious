/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StaticConfigurationChangedExceptionTest {

    @Test
    void humanReadableIdentifiersAccessorReturnsSuppliedNames() {
        var ex = new StaticConfigurationChangedException(Set.of("management", "metrics"));
        assertThat(ex.humanReadableIdentifiers()).containsExactlyInAnyOrder("management", "metrics");
    }

    @Test
    void messageNamesTheIdentifiers() {
        var ex = new StaticConfigurationChangedException(Set.of("management"));
        assertThat(ex.getMessage()).contains("management");
        assertThat(ex.getMessage()).contains("static configuration");
    }

    @Test
    void humanReadableIdentifiersAreImmutable() {
        var ex = new StaticConfigurationChangedException(Set.of("metrics"));
        var identifiers = ex.humanReadableIdentifiers();
        assertThatThrownBy(() -> identifiers.add("admin"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void humanReadableIdentifiersAreDefensivelyCopiedFromCallerSet() {
        var mutable = new HashSet<String>();
        mutable.add("management");
        var ex = new StaticConfigurationChangedException(mutable);

        // Caller mutation of the source must not leak into the exception
        mutable.add("admin");

        assertThat(ex.humanReadableIdentifiers()).containsExactly("management");
    }

    @Test
    void constructorRejectsNullSet() {
        assertThatThrownBy(() -> new StaticConfigurationChangedException(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void isUncheckedRuntimeException() {
        // Documented contract: thrown via future-exceptional-completion, not synchronously.
        // Verifying the type so callers can rely on instanceof checks in whenComplete.
        assertThat(new StaticConfigurationChangedException(Set.of("x"))).isInstanceOf(RuntimeException.class);
    }
}
