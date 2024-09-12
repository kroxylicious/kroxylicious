/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Objects;
import java.util.stream.IntStream;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents the set of integers between two integer endpoints. So the Range
 * with startInclusive 1 and endExclusive 3 contains the integers 1 and 2.
 * A Range must be non-empty, endExclusive must be greater than startInclusive
 */
public record Range(int startInclusive, int endExclusive) {

    /**
     * Constructs a Range
     * @param startInclusive the (inclusive) initial value
     * @param endExclusive the exclusive upper bound
     * @throws IllegalArgumentException if end is before start
     */
    public Range {
        if (endExclusive <= startInclusive) {
            throw new IllegalArgumentException(
                    "end of range: "
                                               + endExclusive
                                               + " (exclusive) is before start of range: "
                                               + startInclusive
                                               + " (inclusive)"
            );
        }
    }

    /**
     * Provides a Stream of values contained in the range
     * @return stream of values
     */
    @NonNull
    public IntStream values() {
        return IntStream.range(startInclusive, endExclusive);
    }

    /**
     * Returns true if this range's end is before the start of another range. ie they do not overlap
     * @param range range to compare
     * @return true if this range ends before the start of other range
     */
    @VisibleForTesting
    boolean isEndBeforeStartOf(@NonNull
    Range range) {
        Objects.requireNonNull(range, "range to compare with is null");
        return this.endExclusive <= range.startInclusive;
    }

    /**
     * Return true if this range does not overlap with other range. If either range
     * ends before the other starts then they must not overlap.
     * @param range range to compare
     * @return true if this range does not overlap with other range
     */
    public boolean isDistinctFrom(@NonNull
    Range range) {
        Objects.requireNonNull(range, "range to compare with is null");
        return isEndBeforeStartOf(range) || range.isEndBeforeStartOf(this);
    }

    @Override
    public String toString() {
        return "[" + startInclusive + "," + endExclusive + ")";
    }
}
