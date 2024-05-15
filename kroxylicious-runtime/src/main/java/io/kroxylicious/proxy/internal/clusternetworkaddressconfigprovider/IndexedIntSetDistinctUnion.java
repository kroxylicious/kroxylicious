/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Union multiple indexed int sets. The Unioned set orders the aggregate set of values and
 * indexes them based on their position. So if we union [4,5] and [1,2], the index of 1 is 0
 * and the index of 4 is 2.</p>
 * <p>Each value in the union must exist in exactly one of the component sets. This
 * differs from a regular set union.</p>
 */
record IndexedIntSetDistinctUnion(Map<Integer, Integer> index) implements IndexedIntSet {

    static IndexedIntSet union(Collection<IndexedIntSet> intIntervals) {
        return new IndexedIntSetDistinctUnion(index(intIntervals));
    }

    static Map<Integer, Integer> index(Collection<IndexedIntSet> intIntervals) {
        int[] ints = intIntervals.stream().flatMapToInt(indexedIntSet -> indexedIntSet.values().stream().mapToInt(value -> value)).distinct().sorted().toArray();
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < ints.length; i++) {
            map.put(ints[i], i);
        }
        int expectedSize = intIntervals.stream().mapToInt(IndexedIntSet::size).sum();
        if (map.size() != expectedSize) {
            throw new IllegalArgumentException("size of union index less that the sum of it's parts, indexes overlap: " + intIntervals);
        }
        return map;
    }

    @Override
    public int indexOf(int integer) {
        return index.getOrDefault(integer, -1);
    }

    @Override
    public boolean contains(int integer) {
        return index.containsKey(integer);
    }

    @Override
    public int size() {
        return index.size();
    }

    @Override
    public Set<Integer> values() {
        return index.keySet().stream().mapToInt(i -> i).boxed().collect(Collectors.toSet());
    }
}
