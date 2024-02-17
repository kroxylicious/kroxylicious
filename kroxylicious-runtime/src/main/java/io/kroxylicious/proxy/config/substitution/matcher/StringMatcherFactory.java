/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.matcher;

/**
 * Provides access to matchers defined in this package.
 *
 * @since 1.3
 */
public final class StringMatcherFactory {

    /**
     * Defines the singleton for this class.
     */
    public static final StringMatcherFactory INSTANCE = new StringMatcherFactory();

    /**
     * Matches no characters.
     */
    private static final AbstractStringMatcher.NoneMatcher NONE_MATCHER = new AbstractStringMatcher.NoneMatcher();

    /**
     * Matches the tab character.
     */
    private static final AbstractStringMatcher.CharMatcher TAB_MATCHER = new AbstractStringMatcher.CharMatcher('\t');

    /**
     * No need to build instances for now.
     */
    private StringMatcherFactory() {
        // empty
    }

    /**
     * Constructor that creates a matcher from a character.
     *
     * @param ch the character to match, must not be null
     * @return a new Matcher for the given char
     */
    public StringMatcher charMatcher(final char ch) {
        return new AbstractStringMatcher.CharMatcher(ch);
    }

    /**
     * Creates a matcher from a string.
     *
     * @param chars the string to match, null or empty matches nothing
     * @return a new Matcher for the given String
     * @since 1.9
     */
    public StringMatcher stringMatcher(final char... chars) {
        final int length = chars != null ? chars.length : 0;
        return length == 0 ? NONE_MATCHER
                : length == 1 ? new AbstractStringMatcher.CharMatcher(chars[0])
                        : new AbstractStringMatcher.CharArrayMatcher(chars);
    }

    /**
     * Creates a matcher from a string.
     *
     * @param str the string to match, null or empty matches nothing
     * @return a new Matcher for the given String
     */
    public StringMatcher stringMatcher(final String str) {
        return str == null || str.isEmpty() ? NONE_MATCHER : stringMatcher(str.toCharArray());
    }

    /**
     * Returns a matcher which matches the tab character.
     *
     * @return a matcher for a tab
     */
    public StringMatcher tabMatcher() {
        return TAB_MATCHER;
    }

}
