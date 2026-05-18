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

class OutOfScopeChangeExceptionTest {

    @Test
    void differingSectionsAccessorReturnsSuppliedNames() {
        var ex = new OutOfScopeChangeException(Set.of("management", "metrics"));
        assertThat(ex.differingSections()).containsExactlyInAnyOrder("management", "metrics");
    }

    @Test
    void messageNamesTheDifferingSections() {
        var ex = new OutOfScopeChangeException(Set.of("management"));
        assertThat(ex.getMessage()).contains("management");
        assertThat(ex.getMessage()).contains("out-of-scope");
    }

    @Test
    void differingSectionsIsImmutable() {
        var ex = new OutOfScopeChangeException(Set.of("metrics"));
        var sections = ex.differingSections();
        assertThatThrownBy(() -> sections.add("admin"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void differingSectionsIsDefensivelyCopiedFromCallerSet() {
        var mutable = new HashSet<String>();
        mutable.add("management");
        var ex = new OutOfScopeChangeException(mutable);

        // Caller mutation of the source must not leak into the exception
        mutable.add("admin");

        assertThat(ex.differingSections()).containsExactly("management");
    }

    @Test
    void constructorRejectsNullSet() {
        assertThatThrownBy(() -> new OutOfScopeChangeException(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void isUncheckedRuntimeException() {
        // Documented contract: thrown via future-exceptional-completion, not synchronously.
        // Verifying the type so callers can rely on instanceof checks in whenComplete.
        assertThat(new OutOfScopeChangeException(Set.of("x"))).isInstanceOf(RuntimeException.class);
    }
}
