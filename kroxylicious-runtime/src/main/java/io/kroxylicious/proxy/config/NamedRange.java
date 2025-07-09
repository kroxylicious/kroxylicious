/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents the inclusive set of integers between two integer endpoints. So the range
 * with start 1 and end 3 contains the integers 1, 2, and 3.
 * A Range must be non-empty, end must be greater than start
 */
public record NamedRange(@NonNull @JsonProperty(required = true) String name,
                         @JsonProperty(required = true) @JsonInclude(JsonInclude.Include.ALWAYS) int start,
                         @JsonProperty(required = true) @JsonInclude(JsonInclude.Include.ALWAYS) int end) {

    /**
     * Constructs a Range
     *
     * @param name range name
     * @param start the inclusive lower value
     * @param end the inclusive upper bound
     * @throws IllegalArgumentException if end is before start
     */
    public NamedRange {
        Objects.requireNonNull(name);
        if (start > end) {
            throw new IllegalArgumentException(
                    "end of range: " + end + " is before start of range: " + start);
        }
    }

    /**
     * Provides a Stream of values contained in the range
     * @return stream of values
     */
    @NonNull
    public IntStream values() {
        return IntStream.rangeClosed(start, end);
    }

    /**
     * Returns true if this range's end is before the start of another range. i.e. they do not overlap
     * @param range range to compare
     * @return true if this range ends before the start of other range
     */
    @VisibleForTesting
    boolean isEndBeforeStartOf(@NonNull NamedRange range) {
        Objects.requireNonNull(range, "range to compare with is null");
        return this.end < range.start;
    }

    /**
     * Return true if this range does not overlap with other range. If either range
     * ends before the other starts then they must not overlap.
     * @param range range to compare
     * @return true if this range does not overlap with other range
     */
    public boolean isDistinctFrom(@NonNull NamedRange range) {
        Objects.requireNonNull(range, "range to compare with is null");
        return isEndBeforeStartOf(range) || range.isEndBeforeStartOf(this);
    }

    @Override
    public String toString() {
        return "NamedRange{" +
                "name='" + name + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }

    public String toIntervalNotationString() {
        return "[" + start + "," + end + "]";
    }
}
