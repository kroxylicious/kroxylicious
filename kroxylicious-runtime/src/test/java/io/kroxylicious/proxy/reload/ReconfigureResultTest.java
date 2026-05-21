/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconfigureResultTest {

    @Test
    void ofEmptyCollectionProducesEmptyResultWithHasErrorsFalse() {
        var result = ReconfigureResult.of(List.of());
        assertThat(result.errors()).isEmpty();
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void ofWithErrorsProducesNonEmptyResultWithHasErrorsTrue() {
        var error = new ReconfigureError("vc-1", new RuntimeException("boom"));
        var result = ReconfigureResult.of(List.of(error));
        assertThat(result.errors()).containsExactly(error);
        assertThat(result.hasErrors()).isTrue();
    }

    @Test
    void ofTakesDefensiveSnapshotOfCallerCollection() {
        var mutable = new ArrayList<ReconfigureError>();
        mutable.add(new ReconfigureError("vc-1", new RuntimeException("first")));
        var result = ReconfigureResult.of(mutable);

        // Caller mutation of the source must not leak into the result
        mutable.add(new ReconfigureError("vc-2", new RuntimeException("added later")));

        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().iterator().next().humanReadableIdentifier()).isEqualTo("vc-1");
    }

    @Test
    void ofReturnsImmutableErrorsCollection() {
        var result = ReconfigureResult.of(List.of());
        var errors = result.errors();
        var extra = new ReconfigureError("x", new RuntimeException());
        assertThatThrownBy(() -> errors.add(extra))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void ofRejectsNullCollection() {
        assertThatThrownBy(() -> ReconfigureResult.of(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void hasErrorsDefaultMethodReflectsErrorsCollection() {
        // Verify the default method uses the interface's errors() method, not some
        // internal field — so custom implementations get the correct hasErrors() for free.
        var error = new ReconfigureError("c", new RuntimeException());
        ReconfigureResult customImpl = () -> List.of(error);
        assertThat(customImpl.hasErrors()).isTrue();

        ReconfigureResult emptyImpl = List::of;
        assertThat(emptyImpl.hasErrors()).isFalse();
    }
}
