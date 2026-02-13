/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.resourceisolation;

public record VersionRange(short fromInclusive, short toInclusive) {
    public VersionRange {
        if (fromInclusive > toInclusive) {
            throw new IllegalArgumentException("fromInclusive must be less than or equal to toInclusive");
        }
    }

    public static VersionRange of(short singleton) {
        return new VersionRange(singleton, singleton);
    }

    public static VersionRange of(short fromInclusive, short toInclusive) {
        return new VersionRange(fromInclusive, toInclusive);
    }

    public boolean contains(short version) {
        return version >= fromInclusive
                && version <= toInclusive;
    }
}
