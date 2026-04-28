/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeResultTest {

    @Test
    void emptyIsIdempotent() {
        assertThat(ChangeResult.EMPTY.isEmpty()).isTrue();
        assertThat(ChangeResult.EMPTY.merge(ChangeResult.EMPTY).isEmpty()).isTrue();
    }

    @Test
    void nonEmptyIsReportedCorrectly() {
        var added = new ChangeResult(Set.of("a"), Set.of(), Set.of());
        var removed = new ChangeResult(Set.of(), Set.of("r"), Set.of());
        var modified = new ChangeResult(Set.of(), Set.of(), Set.of("m"));
        assertThat(added.isEmpty()).isFalse();
        assertThat(removed.isEmpty()).isFalse();
        assertThat(modified.isEmpty()).isFalse();
    }

    @Test
    void mergeUnionsAllThreeBuckets() {
        var a = new ChangeResult(Set.of("a1"), Set.of("r1"), Set.of("m1"));
        var b = new ChangeResult(Set.of("a2"), Set.of("r2"), Set.of("m2"));
        var merged = a.merge(b);
        assertThat(merged.clustersToAdd()).containsExactlyInAnyOrder("a1", "a2");
        assertThat(merged.clustersToRemove()).containsExactlyInAnyOrder("r1", "r2");
        assertThat(merged.clustersToModify()).containsExactlyInAnyOrder("m1", "m2");
    }

    @Test
    void mergeDeduplicatesOverlappingModifies() {
        var a = new ChangeResult(Set.of(), Set.of(), Set.of("x", "y"));
        var b = new ChangeResult(Set.of(), Set.of(), Set.of("y", "z"));
        assertThat(a.merge(b).clustersToModify()).containsExactlyInAnyOrder("x", "y", "z");
    }

    @Test
    void constructorProducesImmutableSets() {
        var mutable = new java.util.HashSet<>(Set.of("a"));
        var result = new ChangeResult(mutable, Set.of(), Set.of());
        mutable.add("b");
        // internal set should be independent of the caller's set
        assertThat(result.clustersToAdd()).containsExactly("a");
    }
}
