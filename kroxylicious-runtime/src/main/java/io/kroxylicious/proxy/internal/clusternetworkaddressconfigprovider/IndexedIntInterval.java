/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A non-empty integer interval. The index for each value is the 0-based position of the
 * value in the interval from smallest to largest.
 * @param startInclusive the inclusive start integer
 * @param endExclusive the exclusive end integer
 */
record IndexedIntInterval(int startInclusive, int endExclusive) implements IndexedIntSet {

    private static final String START_INCLUSIVE = "[";
    private static final String START_EXCLUSIVE = "(";
    private static final String END_INCLUSIVE = "]";
    private static final String END_EXCLUSIVE = ")";
    private static final String INTEGER_GROUP = "(\\d*)";
    private static final String END_INCLUSIVITY_GROUP = "([" + END_EXCLUSIVE + Pattern.quote(END_INCLUSIVE) + "])";
    private static final String START_INCLUSIVITY_GROUP = "([" + Pattern.quote(START_INCLUSIVE) + START_EXCLUSIVE + "])";
    static Pattern INTERVAL_PATTERN = Pattern.compile("^" + START_INCLUSIVITY_GROUP + INTEGER_GROUP + "," + INTEGER_GROUP + END_INCLUSIVITY_GROUP + "$");

    IndexedIntInterval {
        if (endExclusive <= startInclusive) {
            throw new IllegalArgumentException("endExclusive " + endExclusive + " less than or equal to startInclusive " + startInclusive);
        }
    }

    @Override
    public int indexOf(int integer) {
        return contains(integer) ? integer - startInclusive : -1;
    }

    @Override
    public boolean contains(int integer) {
        return integer >= startInclusive && integer < endExclusive;
    }

    @Override
    public int size() {
        return endExclusive - startInclusive;
    }

    enum Inclusivity {
        INCLUSIVE {
            @Override
            public int toStartInclusive(int value) {
                return value;
            }

            @Override
            public int toEndExclusive(int value) {
                return value + 1;
            }
        },
        EXCLUSIVE {
            @Override
            public int toStartInclusive(int value) {
                return value + 1;
            }

            @Override
            public int toEndExclusive(int value) {
                return value;
            }
        };

        public abstract int toStartInclusive(int value);

        public abstract int toEndExclusive(int value);
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
    static IndexedIntSet parse(String intervalSpec) {
        Matcher matcher = INTERVAL_PATTERN.matcher(intervalSpec);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("range string " + intervalSpec + " did not match " + INTERVAL_PATTERN.pattern());
        }

        Inclusivity startInclusivity = toInclusivity(matcher.group(1));
        int startValue = Integer.parseInt(matcher.group(2));
        int endValue = Integer.parseInt(matcher.group(3));
        Inclusivity endInclusivity = toInclusivity(matcher.group(4));
        return new IndexedIntInterval(startInclusivity.toStartInclusive(startValue), endInclusivity.toEndExclusive(endValue));
    }

    private static Inclusivity toInclusivity(String inclusivity) {
        if (inclusivity.equals(START_INCLUSIVE) || inclusivity.equals(END_INCLUSIVE)) {
            return Inclusivity.INCLUSIVE;
        }
        else if (inclusivity.equals(START_EXCLUSIVE) || inclusivity.equals(END_EXCLUSIVE)) {
            return Inclusivity.EXCLUSIVE;
        }
        else {
            throw new IllegalArgumentException("input " + inclusivity + " did not map to an inclusivity value");
        }
    }

    @Override
    public Set<Integer> values() {
        return IntStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toSet());
    }

}
