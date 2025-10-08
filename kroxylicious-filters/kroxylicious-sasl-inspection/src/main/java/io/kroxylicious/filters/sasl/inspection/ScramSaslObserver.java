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

import javax.security.sasl.SaslException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A SASL observer capable of extracting an authorization id from a
 * <a href="https://datatracker.ietf.org/doc/html/rfc5802">SCRAM</a> client first message.
 * <br/>
 * Note that the SCRAM 256 and 512 client first message format is the same, so this observer is
 * good for both.
 */
public class ScramSaslObserver implements SaslObserver {

    private static final String ENCODED_COMMA = "=2C";
    private static final String ENCODED_EQUALS_SIGN = "=3D";

    private final String mechanismName;
    private boolean gotServerFinal = false;
    private @Nullable String authorizationId = null;

    public ScramSaslObserver(String mechanismName) {
        Objects.requireNonNull(mechanismName);
        this.mechanismName = mechanismName;
    }

    @Override
    public boolean clientResponse(byte[] clientFirst) throws SaslException {
        if (authorizationId == null) {
            // n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
            // n,a=ursel,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL

            List<String> tokens = parseScramClientFirst(new String(clientFirst, StandardCharsets.UTF_8));
            var username = tokens.get(2).startsWith("n=") ? tokens.get(2).substring(2) : null;
            if (username == null || username.isEmpty()) {
                throw new SaslException("Invalid SCRAM client first message, username (n) absent");
            }
            var authzid = tokens.get(1).startsWith("a=") ? tokens.get(1).substring(2) : "";
            authorizationId = username(authzid.isEmpty() ? username : authzid);
            return true;
        }
        return false;
    }

    @Override
    public void serverChallenge(byte[] challenge) throws SaslException {
        if (!gotServerFinal) {
            var c = new String(challenge, StandardCharsets.UTF_8);
            boolean verifier = c.startsWith("v=");
            boolean serverError = c.startsWith("e=");
            if (verifier || serverError) {
                gotServerFinal = true;
            }
        }
    }

    @Override
    public String mechanismName() {
        return mechanismName;
    }

    @Override
    public boolean isFinished() {
        return gotServerFinal;
    }

    @Override
    public String authorizationId() throws SaslException {
        if (authorizationId == null) {
            throw new SaslException("SASL SCRAM negotiation has not produced an authorization id");
        }
        return authorizationId;
    }

    private List<String> parseScramClientFirst(String clientFirst) throws SaslException {
        List<String> tokens = new ArrayList<>(4);
        int startIndex = 0;
        for (int i = 0; i < 4; ++i) {
            int endIndex = clientFirst.indexOf(",", startIndex);
            if (endIndex == -1) {
                tokens.add(clientFirst.substring(startIndex));
                break;
            }
            tokens.add(clientFirst.substring(startIndex, endIndex));
            startIndex = endIndex + 1;
        }
        if (tokens.size() != 4) {
            throw new SaslException("Invalid SASL/" + this.mechanismName() + " response: expected 4 tokens, got " +
                    tokens.size());
        }
        return tokens;
    }

    /**
     * Decodes a saslName into a username using the rules described by
     * <a href="https://datatracker.ietf.org/doc/html/rfc5802#section-5.1">RFC-5802</a>.

     * @param saslName sasl name
     * @return user name
     */
    private static String username(String saslName) throws SaslException {
        // The RFC says: "If the server receives a username that contains '=' not followed by either '2C' or '3D', then the
        // server MUST fail the authentication"
        if (saslName.replace(ENCODED_COMMA, "").replace(ENCODED_EQUALS_SIGN, "").contains("=")) {
            throw new SaslException("Sasl name: '" + saslName + "' contains unexpected encoded characters");
        }

        return saslName.replace(ENCODED_COMMA, ",").replace(ENCODED_EQUALS_SIGN, "=");
    }

}
