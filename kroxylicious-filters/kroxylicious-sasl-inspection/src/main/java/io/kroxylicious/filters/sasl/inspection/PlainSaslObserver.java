/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.errors.SaslAuthenticationException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A SASL observer capable of extracting an authorization id from a
 * <a href="https://tools.ietf.org/html/rfc4616">SASL PLAIN</a> client initial response.
 */
class PlainSaslObserver implements SaslObserver {
    private boolean gotExpectedServerFinal;
    @Nullable
    private String authorizationId;

    @Override
    public boolean clientResponse(byte[] response) {
        Objects.requireNonNull(response, "response");
        if (authorizationId == null) {
            var tokens = parsePlainClient(new String(response, StandardCharsets.UTF_8));
            var optionalAuthzid = tokens.get(0);
            var authcid = tokens.get(1);
            if (authcid.isEmpty()) {
                throw new SaslAuthenticationException("PLAIN saw client initial response with empty authcid.");
            }
            authorizationId = optionalAuthzid.isEmpty() ? authcid : optionalAuthzid;
            return true;
        }
        else {
            throw new SaslAuthenticationException("PLAIN saw unexpected initial client response");
        }
    }

    @Override
    public void serverChallenge(byte[] challenge) {
        if (challenge.length == 0) {
            gotExpectedServerFinal = true;
        }
        else {
            throw new SaslAuthenticationException("PLAIN saw unexpected server challenge");
        }
    }

    @Override
    public String mechanismName() {
        return "PLAIN";
    }

    @Override
    public boolean isFinished() {
        return gotExpectedServerFinal;
    }

    @Override
    public String authorizationId() {
        if (authorizationId == null) {
            throw new SaslAuthenticationException("SASL plain negotiation has not produced an authorization id");
        }
        return authorizationId;
    }

    /* This function originally copied from Apach Kafka's PlainSaslServer */
    private static List<String> parsePlainClient(String string) {
        /*
         * Message format (from https://tools.ietf.org/html/rfc4616):
         *
         * message = [authzid] UTF8NUL authcid UTF8NUL passwd
         * authcid = 1*SAFE ; MUST accept up to 255 octets
         * authzid = 1*SAFE ; MUST accept up to 255 octets
         * passwd = 1*SAFE ; MUST accept up to 255 octets
         * UTF8NUL = %x00 ; UTF-8 encoded NUL character
         *
         * SAFE = UTF1 / UTF2 / UTF3 / UTF4
         * ;; any UTF-8 encoded Unicode character except NUL
         */
        List<String> tokens = new ArrayList<>();
        int startIndex = 0;
        for (int i = 0; i < 4; ++i) {
            int endIndex = string.indexOf("\u0000", startIndex);
            if (endIndex == -1) {
                tokens.add(string.substring(startIndex));
                break;
            }
            tokens.add(string.substring(startIndex, endIndex));
            startIndex = endIndex + 1;
        }

        if (tokens.size() != 3) {
            throw new SaslAuthenticationException("Invalid SASL/PLAIN response: expected 3 tokens, got " +
                    tokens.size());
        }

        return tokens;
    }

}
