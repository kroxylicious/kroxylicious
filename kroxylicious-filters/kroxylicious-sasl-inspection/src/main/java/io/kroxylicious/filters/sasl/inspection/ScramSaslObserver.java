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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A SASL observer capable of extracting an authorization id from a
 * <a href="https://datatracker.ietf.org/doc/html/rfc5802">SCRAM</a> client first message.
 * <br/>
 * Note that the SCRAM 256 and 512 client first message format is the same, so this observer is
 * good for both.
 */
public class ScramSaslObserver implements SaslObserver {
    private final String mechanismName;
    private boolean gotServerFinal;
    private @Nullable String authorizationId;

    public ScramSaslObserver(String mechanismName) {
        Objects.requireNonNull(mechanismName);
        this.mechanismName = mechanismName;
    }

    @Override
    public boolean clientResponse(byte[] clientFirst) {
        if (authorizationId == null) {
            // n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
            // n,a=ursel,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL

            List<String> tokens = parseScramClientFirst(new String(clientFirst, StandardCharsets.UTF_8));
            var username = tokens.get(2).startsWith("n=") ? tokens.get(2).substring(2) : null;
            if (username == null || username.isEmpty()) {
                throw new SaslAuthenticationException("Invalid SCRAM client first message, username (n) absent");
            }
            var authzid = tokens.get(1).startsWith("a=") ? tokens.get(1).substring(2) : "";
            authorizationId = ScramFormatter.username(authzid.isEmpty() ? username : authzid);
            return true;
        }
        return false;
    }

    @Override
    public void serverChallenge(byte[] challenge) {
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
    public String authorizationId() throws AuthenticationException {
        if (authorizationId == null) {
            throw new SaslAuthenticationException("SASL SCRAM negotiation has not produced an authorization id");
        }
        return authorizationId;
    }

    private List<String> parseScramClientFirst(String clientFirst) {
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
            throw new SaslAuthenticationException("Invalid SASL/" + this.mechanismName() + " response: expected 4 tokens, got " +
                    tokens.size());
        }
        return tokens;
    }

}
