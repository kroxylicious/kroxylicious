/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.matcher;

/**
 * Provides access to matchers defined in this package.
 *
 */
public final class StringMatcherFactory {

    /**
     * Defines the singleton for this class.
     */
    public static final StringMatcherFactory INSTANCE = new StringMatcherFactory();

    /**
     * Matches no characters.
     */
    private static final NoneMatcher NONE_MATCHER = new NoneMatcher();

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
        return new CharMatcher(ch);
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
                : length == 1 ? new CharMatcher(chars[0])
                        : new CharArrayMatcher(chars);
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
     * Matches a character.
     * <p>
     * Thread=safe.
     * </p>
     */
    private static final class CharMatcher implements StringMatcher {

        /** The character to match. */
        private final char ch;

        /**
         * Constructs a matcher for a single character.
         *
         * @param ch the character to match
         */
        CharMatcher(final char ch) {
            this.ch = ch;
        }

        /**
         * Returns {@code 1} if there is a match, or {@code 0} if there is no match.
         *
         * @param buffer the text content to match against, do not change
         * @param start the starting position for the match, valid for buffer
         * @param bufferStart unused
         * @param bufferEnd unused
         * @return The number of matching characters, zero for no match
         */
        @Override
        public int isMatch(final char[] buffer, final int start, final int bufferStart, final int bufferEnd) {
            return ch == buffer[start] ? 1 : 0;
        }

        /**
         * Returns {@code 1} if there is a match, or {@code 0} if there is no match.
         *
         * @param buffer the text content to match against, do not change
         * @param start the starting position for the match, valid for buffer
         * @param bufferStart unused
         * @param bufferEnd unused
         * @return The number of matching characters, zero for no match
         */
        @Override
        public int isMatch(final CharSequence buffer, final int start, final int bufferStart, final int bufferEnd) {
            return ch == buffer.charAt(start) ? 1 : 0;
        }

        /**
         * Returns 1.
         *
         * @since 1.9
         */
        @Override
        public int size() {
            return 1;
        }

        @Override
        public String toString() {
            return super.toString() + "['" + ch + "']";
        }
    }

    /**
     * Matches nothing.
     * <p>
     * Thread=safe.
     * </p>
     */
    private static final class NoneMatcher implements StringMatcher {

        /**
         * Constructs a new instance of {@code NoMatcher}.
         */
        NoneMatcher() {
        }

        /**
         * Always returns {@code 0}.
         *
         * @param buffer unused
         * @param start unused
         * @param bufferStart unused
         * @param bufferEnd unused
         * @return The number of matching characters, zero for no match
         */
        @Override
        public int isMatch(final char[] buffer, final int start, final int bufferStart, final int bufferEnd) {
            return 0;
        }

        /**
         * Always returns {@code 0}.
         *
         * @param buffer unused
         * @param start unused
         * @param bufferStart unused
         * @param bufferEnd unused
         * @return The number of matching characters, zero for no match
         */
        @Override
        public int isMatch(final CharSequence buffer, final int start, final int bufferStart, final int bufferEnd) {
            return 0;
        }

    }

    /**
     * Matches out of a set of characters.
     * <p>
     * Thread=safe.
     * </p>
     */
    private static final class CharArrayMatcher implements StringMatcher {

        /** The string to match, as a character array, implementation treats as immutable. */
        private final char[] chars;

        /** The string to match. */
        private final String string;

        /**
         * Constructs a matcher from a String.
         *
         * @param chars the string to match, must not be null
         */
        CharArrayMatcher(final char... chars) {
            this.string = String.valueOf(chars);
            this.chars = chars.clone();
        }

        /**
         * Returns the number of matching characters, {@code 0} if there is no match.
         *
         * @param buffer the text content to match against, do not change
         * @param start the starting position for the match, valid for buffer
         * @param bufferStart unused
         * @param bufferEnd the end index of the active buffer, valid for buffer
         * @return The number of matching characters, zero for no match
         */
        @Override
        public int isMatch(final char[] buffer, final int start, final int bufferStart, final int bufferEnd) {
            final int len = size();
            if (start + len > bufferEnd) {
                return 0;
            }
            int j = start;
            for (int i = 0; i < len; i++, j++) {
                if (chars[i] != buffer[j]) {
                    return 0;
                }
            }
            return len;
        }

        /**
         * Returns the number of matching characters, {@code 0} if there is no match.
         *
         * @param buffer the text content to match against, do not change
         * @param start the starting position for the match, valid for buffer
         * @param bufferStart unused
         * @param bufferEnd the end index of the active buffer, valid for buffer
         * @return The number of matching characters, zero for no match
         */
        @Override
        public int isMatch(final CharSequence buffer, final int start, final int bufferStart, final int bufferEnd) {
            final int len = size();
            if (start + len > bufferEnd) {
                return 0;
            }
            int j = start;
            for (int i = 0; i < len; i++, j++) {
                if (chars[i] != buffer.charAt(j)) {
                    return 0;
                }
            }
            return len;
        }

        /**
         * Returns the size of the string to match given in the constructor.
         *
         * @since 1.9
         */
        @Override
        public int size() {
            return chars.length;
        }

        @Override
        public String toString() {
            return super.toString() + "[\"" + string + "\"]";
        }
    }
}
