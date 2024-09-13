/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type Version comparator.
 */
public class VersionComparator implements Comparable<String> {
    private final String currentVersion;

    /**
     * Instantiates a new Version comparator.
     *
     * @param version the version
     */
    public VersionComparator(String version) {
        this.currentVersion = version;
    }

    @Override
    public int compareTo(
            @NonNull
            String expectedVersion
    ) {
        String[] currentParts = this.currentVersion.split("\\.");
        String[] expectedParts = expectedVersion.split("\\.");

        for (int i = 0; i < expectedParts.length; i++) {
            int currentPart = i < currentParts.length ? Integer.parseInt(currentParts[i]) : 0;
            int expectedPart = Integer.parseInt(expectedParts[i]);
            int comparison = Integer.compare(currentPart, expectedPart);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        // self check
        if (this == o) {
            return true;
        }

        // null check
        if (o == null) {
            return false;
        }

        // type check and cast
        if (getClass() != o.getClass()) {
            return false;
        }

        return ((VersionComparator) o).currentVersion.equals(this.currentVersion);
    }

    @Override
    public int hashCode() {
        return currentVersion.hashCode();
    }
}
