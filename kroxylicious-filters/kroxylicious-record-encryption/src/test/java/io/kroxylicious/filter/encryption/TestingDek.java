/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Arrays;

public record TestingDek(@SuppressWarnings("ArrayRecordComponent") byte[] serializedEdek) { // byte[] retained: test-only record; deep equality via explicit equals/hashCode below

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestingDek that)) {
            return false;
        }
        return Arrays.equals(serializedEdek, that.serializedEdek);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedEdek);
    }

    @Override
    public String toString() {
        return "TestingDek{" +
                "serializedEdek=" + Arrays.toString(serializedEdek) +
                '}';
    }
}
