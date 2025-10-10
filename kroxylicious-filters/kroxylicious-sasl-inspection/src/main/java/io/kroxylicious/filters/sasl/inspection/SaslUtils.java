/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

final class SaslUtils {

    private static final String ENCODED_COMMA = "=2C";
    private static final String ENCODED_EQUALS_SIGN = "=3D";

    private SaslUtils() {

    }

    /**
     * Decodes a saslName into a username using the rules described by
     * <a href="https://datatracker.ietf.org/doc/html/rfc5802#section-5.1">RFC-5802</a>.
     * @param saslName sasl name
     * @return user name
     * @throws IllegalArgumentException sasl name contains unexpected encoded characters.
     */
    public static String username(String saslName) throws IllegalArgumentException {
        // The RFC says: "If the server receives a username that contains '=' not followed by either '2C' or '3D', then the
        // server MUST fail the authentication"
        if (saslName.replace(ENCODED_COMMA, "").replace(ENCODED_EQUALS_SIGN, "").contains("=")) {
            throw new IllegalArgumentException("Sasl name: '" + saslName + "' contains unexpected encoded characters");
        }

        return saslName.replace(ENCODED_COMMA, ",").replace(ENCODED_EQUALS_SIGN, "=");
    }
}
