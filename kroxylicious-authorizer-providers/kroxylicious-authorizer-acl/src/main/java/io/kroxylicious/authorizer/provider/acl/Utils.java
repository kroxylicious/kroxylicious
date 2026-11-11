/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

/**
 * Helpers for reading ACL rules files.
 */
final class Utils {

    private Utils() {
    }

    /**
     * Removes the ACL escapes from the body of a regex literal: {@code \/} is a slash and {@code \\} a
     * backslash. Any other backslash is kept, because it belongs to the regular expression:
     * {@code \.} stays an escaped dot, and {@code \d} a digit class.
     *
     * @param aclEscapedRe the regex literal without its {@code /} delimiters.
     * @return the regular expression to compile.
     */
    static String unquoteRegex(String aclEscapedRe) {
        var regex = new StringBuilder(aclEscapedRe.length());
        int i = 0;
        while (i < aclEscapedRe.length()) {
            char ch = aclEscapedRe.charAt(i);
            boolean aclEscape = ch == '\\'
                    && i + 1 < aclEscapedRe.length()
                    && (aclEscapedRe.charAt(i + 1) == '/' || aclEscapedRe.charAt(i + 1) == '\\');
            if (aclEscape) {
                regex.append(aclEscapedRe.charAt(i + 1));
                i += 2;
            }
            else {
                regex.append(ch);
                i++;
            }
        }
        return regex.toString();
    }
}
