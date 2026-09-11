/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

/**
 * Argument-checking assertions.
 */
public class Assertions {
    private Assertions() {
    }

    /**
     * Checks that the given number is strictly positive.
     *
     * @param num the number to check
     * @param what a description of the number, used in the exception message
     * @return the number
     * @throws IllegalArgumentException if the number is zero or negative
     */
    public static long requireStrictlyPositive(long num, String what) {
        if (num <= 0) {
            throw new IllegalArgumentException(what + " must be > 0");
        }
        return num;
    }

    /**
     * Checks that the given number is zero or positive.
     *
     * @param num the number to check
     * @param what a description of the number, used in the exception message
     * @return the number
     * @throws IllegalArgumentException if the number is negative
     */
    public static long requirePositive(long num, String what) {
        if (num < 0) {
            throw new IllegalArgumentException(what + " must to be >= 0");
        }
        return num;
    }
}
