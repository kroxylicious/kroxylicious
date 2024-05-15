/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexedIntSetDistinctUnionTest {

    @Test
    void indexOf() {
        IndexedIntSet union = IndexedIntSet.distinctUnion(Set.of(new IndexedIntInterval(1, 3), new IndexedIntInterval(4, 6)));
        assertThat(union.indexOf(0)).isEqualTo(-1);
        assertThat(union.indexOf(1)).isEqualTo(0);
        assertThat(union.indexOf(2)).isEqualTo(1);
        assertThat(union.indexOf(3)).isEqualTo(-1);
        assertThat(union.indexOf(4)).isEqualTo(2);
        assertThat(union.indexOf(5)).isEqualTo(3);
        assertThat(union.indexOf(6)).isEqualTo(-1);
    }

    @Test
    void contains() {
        IndexedIntSet union = IndexedIntSet.distinctUnion(Set.of(new IndexedIntInterval(1, 3), new IndexedIntInterval(4, 6)));
        assertThat(union.contains(0)).isFalse();
        assertThat(union.contains(1)).isTrue();
        assertThat(union.contains(2)).isTrue();
        assertThat(union.contains(3)).isFalse();
        assertThat(union.contains(4)).isTrue();
        assertThat(union.contains(5)).isTrue();
        assertThat(union.contains(6)).isFalse();
    }

    @Test
    void size() {
        IndexedIntSet union = IndexedIntSet.distinctUnion(Set.of(new IndexedIntInterval(1, 3), new IndexedIntInterval(4, 6)));
        assertThat(union.size()).isEqualTo(4);
    }

    @Test
    void distinctness() {
        Set<IndexedIntSet> sets = Set.of(new IndexedIntInterval(1, 3), new IndexedIntInterval(2, 4));
        assertThatThrownBy(() -> IndexedIntSet.distinctUnion(sets))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size of union index less that the sum of it's parts, indexes overlap");
    }
}