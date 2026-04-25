/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyedListEqualityTest {

    private record Named(String key, String payload) {}

    private static final Function<Named, String> BY_KEY = Named::key;

    @Test
    void equalReturnsTrueForSameListsInDifferentOrder() {
        var a = List.of(new Named("a", "1"), new Named("b", "2"));
        var b = List.of(new Named("b", "2"), new Named("a", "1"));
        assertThat(KeyedListEquality.equal(a, b, BY_KEY)).isTrue();
    }

    @Test
    void equalReturnsFalseWhenPayloadDiffers() {
        var a = List.of(new Named("a", "1"));
        var b = List.of(new Named("a", "2"));
        assertThat(KeyedListEquality.equal(a, b, BY_KEY)).isFalse();
    }

    @Test
    void equalReturnsFalseWhenKeysetDiffers() {
        var a = List.of(new Named("a", "1"));
        var b = List.of(new Named("z", "1"));
        assertThat(KeyedListEquality.equal(a, b, BY_KEY)).isFalse();
    }

    @Test
    void equalReturnsFalseWhenSizesDiffer() {
        var a = List.of(new Named("a", "1"));
        var b = List.of(new Named("a", "1"), new Named("b", "2"));
        assertThat(KeyedListEquality.equal(a, b, BY_KEY)).isFalse();
    }

    @Test
    void equalBothNullReturnsTrue() {
        assertThat(KeyedListEquality.equal(null, null, BY_KEY)).isTrue();
    }

    @Test
    void equalOneNullReturnsFalse() {
        var list = List.of(new Named("a", "1"));
        assertThat(KeyedListEquality.equal(list, null, BY_KEY)).isFalse();
        assertThat(KeyedListEquality.equal(null, list, BY_KEY)).isFalse();
    }

    @Test
    void equalBothEmptyReturnsTrue() {
        assertThat(KeyedListEquality.equal(List.<Named> of(), List.<Named> of(), BY_KEY)).isTrue();
    }

    @Test
    void equalRejectsDuplicateKeys() {
        // Same-size lists so the size-mismatch shortcut doesn't bypass the duplicate-key check.
        var a = List.of(new Named("a", "1"), new Named("a", "2"));
        var b = List.of(new Named("a", "1"), new Named("b", "2"));
        assertThatThrownBy(() -> KeyedListEquality.equal(a, b, BY_KEY))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void changedKeysReportsNoDifferenceForReorderedList() {
        var a = List.of(new Named("a", "1"), new Named("b", "2"));
        var b = List.of(new Named("b", "2"), new Named("a", "1"));
        assertThat(KeyedListEquality.changedKeys(a, b, BY_KEY)).isEmpty();
    }

    @Test
    void changedKeysReportsAdditions() {
        var a = List.of(new Named("a", "1"));
        var b = List.of(new Named("a", "1"), new Named("b", "2"));
        assertThat(KeyedListEquality.changedKeys(a, b, BY_KEY)).containsExactly("b");
    }

    @Test
    void changedKeysReportsRemovals() {
        var a = List.of(new Named("a", "1"), new Named("b", "2"));
        var b = List.of(new Named("a", "1"));
        assertThat(KeyedListEquality.changedKeys(a, b, BY_KEY)).containsExactly("b");
    }

    @Test
    void changedKeysReportsModifications() {
        var a = List.of(new Named("a", "1"));
        var b = List.of(new Named("a", "2"));
        assertThat(KeyedListEquality.changedKeys(a, b, BY_KEY)).containsExactly("a");
    }

    @Test
    void changedKeysCombinesAllThree() {
        var a = List.of(new Named("keep", "x"), new Named("mod", "v1"), new Named("gone", "g"));
        var b = List.of(new Named("keep", "x"), new Named("mod", "v2"), new Named("new", "n"));
        assertThat(KeyedListEquality.changedKeys(a, b, BY_KEY))
                .containsExactlyInAnyOrder("mod", "gone", "new");
    }

    @Test
    void changedKeysHandlesNullsAsEmpty() {
        assertThat(KeyedListEquality.changedKeys(null, null, BY_KEY)).isEmpty();
        var list = List.of(new Named("a", "1"));
        assertThat(KeyedListEquality.changedKeys(null, list, BY_KEY)).containsExactly("a");
        assertThat(KeyedListEquality.changedKeys(list, null, BY_KEY)).containsExactly("a");
    }
}
