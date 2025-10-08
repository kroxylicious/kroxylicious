/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.errors.AuthenticationException;

/**
 * A Sasl observer merely watches a SASL negotiation between client and server extracting the
 * authorization id.  It is also responsible for signalling when the SASL negotiation
 * is finished.
 * <br/>
 * Unlike a {@link javax.security.sasl.SaslServer}, it does not if the authentication was successful or not.
 */
public interface SaslObserver {

    /**
     * IANA registered SASL mechanism name.
     *
     * @return SASL mechanism name.
     */
    String mechanismName();

    /**
     * Used to inform the observer of the bytes of each client response.
     *
     * @param response client response
     * @return true if this response yields the authorization id, false otherwise.
     * @throws AuthenticationException if the response is incorrectly formatted
     */
    boolean clientResponse(byte[] response) throws SaslException;

    /**
     * Used to inform the observer of the bytes of each server response.
     *
     * @param challenge server challenge
     * @throws AuthenticationException if the challenge is incorrectly formatted
     */
    void serverChallenge(byte[] challenge) throws SaslException;

    /**
     * Reports if the SASL negotiation is finished.  A negotiation is finished once the server
     * has sent its last challenge.
     *
     * @return true if the SASL negotiation is completed, false otherwise.
     */
    boolean isFinished();

    /**
     * Returns the negotiated authorization identity
     * @return negotiated authorization identity.
     *
     * @throws AuthenticationException if the authorization identity has not been established.
     */
    String authorizationId() throws SaslException;
}
