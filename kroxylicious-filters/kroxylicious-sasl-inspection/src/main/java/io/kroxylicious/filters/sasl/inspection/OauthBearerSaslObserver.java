/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Optional;
import java.util.function.Predicate;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.jose4j.jwt.GeneralJwtException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A SASL observer capable of extracting an authorization id from a
 * <a href="https://datatracker.ietf.org/doc/html/rfc7628">OAUTHBEARER</a> initial client response.
 * <br/>
 * The authorization id is obtained in the following manner.
 * <ol>
 *     <li>If the authorization id is present in the <a href="https://www.rfc-editor.org/rfc/rfc5801.html#page-8">gs2-header</a> it is used.</li>
 *     <li>If there is no authorization id in the header, the subject claim in the JWT payload is used.</li>
 *     <li>If the subject claim is absent, the payload is encrypted (JWE), or opaque tokens are in use, the authentication will fail.</li>
 * </ol>
 */
class OauthBearerSaslObserver implements SaslObserver {

    private boolean gotExpectedServerFinal = false;

    private boolean gotServerError = false;
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
            var authzid = Optional.of(initialResponse.authorizationId()).filter(Predicate.not(String::isEmpty)).map(SaslUtils::username);

            try {
                if (authzid.isEmpty()) {
                    // It is not the role of the observer to validate the JWT - the server
                    // will decide if it is acceptable. We merely need to extract
                    // the subject claim.
                    var consumer = new JwtConsumerBuilder()
                            .setSkipAllValidators()
                            .setDisableRequireSignature()
                            .setSkipSignatureVerification()
                            .build();
                    var context = consumer.process(initialResponse.tokenValue());
                    var sub = context.getJwtClaims().getStringClaimValue("sub");

                    authorizationId = Optional.ofNullable(sub).filter(Predicate.not(String::isEmpty))
                            .orElseThrow(() -> new SaslException("JWT does not contain subject claim (sub)"));
                }
                else {
                    authorizationId = authzid.get();
                }
                return true;
            }
            catch (IllegalArgumentException | GeneralJwtException | InvalidJwtException e) {
                throw new SaslException("Unacceptable initial client response", e);
            }
        }
        else if (gotServerError && isControlA(response)) {
            // this is the expected client response to the server's error challenge.
            return false;
        }
        else {
            throw new SaslException("Received an unexpected client response");
        }
    }

    private boolean isControlA(byte[] response) {
        return (response.length == 1 && response[0] == 1);
    }

    @Override
    public void serverChallenge(byte[] challenge) throws SaslException {
        if (challenge.length == 0) {
            gotExpectedServerFinal = true;
        }
        else if (!gotServerError) {
            gotServerError = true;
        }
        else {
            throw new SaslException("Received an unexpected server challenge");
        }
    }

    @Override
    public boolean isFinished() {
        return gotExpectedServerFinal;
    }

    @Override
    public String authorizationId() throws SaslException {
        if (authorizationId == null) {
            throw new SaslException("SASL OAUTHBEARER negotiation has not produced an authorization id");
        }
        return authorizationId;
    }

    @Override
    public boolean usesSessionLifetime() {
        return true;
    }
}
