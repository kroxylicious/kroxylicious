/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.lookup;

import org.apache.commons.lang3.StringUtils;

/**
 * A default lookup for others to extend in this package.
 *
 * @since 1.3
 */
abstract class AbstractStringLookup implements StringLookup {

    /**
     * The default split char.
     */
    protected static final char SPLIT_CH = ':';

    /**
     * The default split string.
     */
    protected static final String SPLIT_STR = String.valueOf(SPLIT_CH);

    /**
     * Creates a lookup key for a given file and key.
     */
    static String toLookupKey(final String left, final String right) {
        return toLookupKey(left, SPLIT_STR, right);
    }

    /**
     * Creates a lookup key for a given file and key.
     */
    static String toLookupKey(final String left, final String separator, final String right) {
        return left + separator + right;
    }

    /**
     * Returns the substring after the first occurrence of {@code ch} in {@code value}.
     *
     * @param value The source string.
     * @param ch The character to search.
     * @return a new string.
     * @deprecated Use {@link StringUtils#substringAfter(String, int)}.
     */
    @Deprecated
    protected String substringAfter(final String value, final char ch) {
        return StringUtils.substringAfter(value, ch);
    }

    /**
     * Returns the substring after the first occurrence of {@code str} in {@code value}.
     *
     * @param value The source string.
     * @param str The string to search.
     * @return a new string.
     * @deprecated Use {@link StringUtils#substringAfter(String, String)}.
     */
    @Deprecated
    protected String substringAfter(final String value, final String str) {
        return StringUtils.substringAfter(value, str);
    }

    /**
     * Returns the substring after the first occurrence of {@code ch} in {@code value}.
     *
     * @param value The source string.
     * @param ch The character to search.
     * @return a new string.
     * @deprecated Use {@link StringUtils#substringAfterLast(String, int)}.
     */
    @Deprecated
    protected String substringAfterLast(final String value, final char ch) {
        return StringUtils.substringAfterLast(value, ch);
    }

}
