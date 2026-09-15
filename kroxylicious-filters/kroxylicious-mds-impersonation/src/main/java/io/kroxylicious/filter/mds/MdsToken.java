/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Token obtained from trusted MDS; the Kafka broker performs signature validation. */
record MdsToken(String value, Instant expiresAt) {
    private static final Pattern JWT = Pattern.compile("[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+");

    static MdsToken parse(byte[] response, ObjectMapper mapper) {
        try {
            JsonNode root = mapper.readTree(response);
            if (root == null || !"Bearer".equalsIgnoreCase(root.path("token_type").asText())) {
                throw new IllegalArgumentException();
            }
            String value = root.path("auth_token").asText();
            if (value.length() > 32768 || !JWT.matcher(value).matches()) {
                throw new IllegalArgumentException();
            }
            JsonNode claims = mapper.readTree(Base64.getUrlDecoder().decode(value.split("\\.")[1]));
            JsonNode exp = claims == null ? null : claims.get("exp");
            if (exp == null || !exp.isIntegralNumber() || !exp.canConvertToLong() || exp.longValue() <= 0) {
                throw new IllegalArgumentException();
            }
            return new MdsToken(value, Instant.ofEpochSecond(exp.longValue()));
        }
        catch (IOException | RuntimeException e) {
            // JSON parser exceptions can contain the token. Do not retain their messages or causes.
            throw new IllegalArgumentException("MDS returned an invalid bearer token");
        }
    }

    byte[] saslResponse() {
        // RFC 7628 initial client response, without an authorization identity or extensions.
        return ("n,,\u0001auth=Bearer " + value + "\u0001\u0001").getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "MdsToken[value=<redacted>, expiresAt=" + expiresAt + "]";
    }
}
