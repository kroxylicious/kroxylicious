/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Base64;
import java.util.Optional;
import java.util.function.Predicate;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A Sasl Observer supporting SASL mechanism OAUTHBEARER described by <a href="https://datatracker.ietf.org/doc/html/rfc7628">RFC-7628</a>.
 */
class OauthBearerSaslObserver implements SaslObserver {

    public static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();
    private boolean gotExpectedServerFinal = false;

    @Nullable
    private String authorizationId = null;

    @Override
    public String mechanismName() {
        return "OAUTHBEARER";
    }

    @Override
    public boolean clientResponse(byte[] response) throws SaslException {
        if (authorizationId == null) {
            var initialResponse = new OAuthBearerClientInitialResponse(response);
            var authzid = Optional.of(initialResponse.authorizationId()).filter(Predicate.not(String::isEmpty));

            if (authzid.isEmpty()) {
                var chunks = initialResponse.tokenValue().split("\\.");
                String payload = new String(URL_DECODER.decode(chunks[1]));
                try {
                    JsonNode tree = new ObjectMapper().readTree(payload);
                    var sub = tree.get("sub").asText();
                    authorizationId = Optional.of(initialResponse.authorizationId()).filter(Predicate.not(String::isEmpty)).orElse(sub);
                    return true;
                }
                catch (JsonProcessingException e) {
                    throw new SaslException("failed to read payload", e);
                }
            }
            else {
                authorizationId = authzid.get();
                return true;
            }
        }
        else {
            throw new SaslException("OAUTHBEARER saw an unexpected client response");
        }
    }

    @Override
    public void serverChallenge(byte[] challenge) throws SaslException {
        gotExpectedServerFinal = true;
    }

    @Override
    public boolean isFinished() {
        return gotExpectedServerFinal;
    }

    @Override
    public String authorizationId() throws SaslException {
        if (authorizationId == null) {
            throw new SaslException("SASL plain negotiation has not produced an authorization id");
        }
        return authorizationId;
    }
}
