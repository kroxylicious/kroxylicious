/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Collection;
import java.util.Set;

/**
 * Represents a set of distinct integers with a stable index from each of its values
 * to a distinct integer in the range [0, size). Any set of integers can build a
 * deterministic index by sorting the values and then using the position of each value
 * as its index.
 */
public interface IndexedIntSet {

    /**
     * Returns the index for this value. Each value will have a unique index in the
     * range [0, size).
     * @param integer value
     * @return index index for value
     */
    int indexOf(int integer);

    /**
     * Returns true if this set contains the specified integer.
     * @param integer integer whose presence in this collection is to be tested
     * @return true if this set contains the specified element
     */
    boolean contains(int integer);

    /**
     * Returns the number of integers in this set
     * @return the number of integers in this set
     */
    int size();

    /**
     * Returns the set of values
     * @return values
     */
    Set<Integer> values();

    /**
     * <p>Union multiple indexed int sets. The Unioned set orders the aggregate set of values and
     * indexes them based on their position. So if we union [4,5] and [1,2], the index of 1 is 0
     * and the index of 4 is 2.</p>
     * <p>Each value in the union must exist in exactly one of the component sets. This
     * differs from a regular set union.</p>
     * @param sets indexed int sets to be unioned
     * @return IndexedIntSet the union set
     * @throws IllegalArgumentException if any integer is present in more than one input set
     */
    static IndexedIntSet distinctUnion(Collection<IndexedIntSet> sets) {
        return IndexedIntSetDistinctUnion.union(sets);
    }

    /**
     * Parse an integer range spec like [0,1). Square brackets are inclusive, round are exclusive. e.g.
     * <ul>
     *     <li>[1,3] -> (1,2,3)</li>
     *     <li>(1,3] -> (2,3)</li>
     *     <li>[1,3) -> (1,2)</li>
     *     <li>(1,3) -> (2)</li>
     * </ul>
     * @param intervalSpec range spec string
     * @return IndexedIntSet for specified range
     * @throws IllegalArgumentException if the end of the range is before the start, e.g. [1,1) or [2,1]
     */
    static IndexedIntSet parseInterval(String intervalSpec) {
        return IndexedIntInterval.parse(intervalSpec);
    }
}
