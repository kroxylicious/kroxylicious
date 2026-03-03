/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

public class Assertions {
    private Assertions() {
    }

    public static long requireStrictlyPositive(long num, String what) {
        if (num <= 0) {
            throw new IllegalArgumentException(what + " must be > 0");
        }
        return num;
    }

    public static long requirePositive(long num, String what) {
        if (num < 0) {
            throw new IllegalArgumentException(what + " must to be >= 0");
        }
        return num;
    }
}
