/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

/**
 * Handles OAUTHBEARER authentication using JWT bearer tokens.
 * <p>
 * Uses Kafka's {@link SaslServer} implementation with
 * {@link OAuthBearerValidatorCallbackHandler} to validate JWT tokens
 * against a JWKS endpoint.
 * </p>
 *
 * <h2>Authentication Flow</h2>
 * <p>
 * OAUTHBEARER (RFC 7628) is typically single-round:
 * </p>
 * <ol>
 *     <li>Client sends initial response containing the bearer token</li>
 *     <li>Server validates the token and responds with success or failure</li>
 * </ol>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Not thread-safe. Each instance is used for a single connection
 * on that connection's event loop thread.
 * </p>
 */
class OauthBearerStateMachine implements MechanismStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(OauthBearerStateMachine.class);

    private final OAuthBearerValidatorCallbackHandler callbackHandler;
    private final int maxAuthBytes;

    @Nullable
    private SaslServer saslServer;

    /**
     * Create an OAUTHBEARER state machine.
     *
     * @param callbackHandler the configured callback handler for JWT validation
     * @param maxAuthBytes maximum auth payload size in bytes
     */
    OauthBearerStateMachine(OAuthBearerValidatorCallbackHandler callbackHandler, int maxAuthBytes) {
        this.callbackHandler = Objects.requireNonNull(callbackHandler);
        this.maxAuthBytes = maxAuthBytes;
    }

    @Override
    public String mechanismName() {
        return OAUTHBEARER_MECHANISM;
    }

    // Limits the processing cost (JWT parsing, JWKS validation) of oversized payloads.
    // This is defense-in-depth: the byte[] is already allocated from the network buffer
    // before the filter is called, so this cannot prevent the allocation itself.
    @Override
    public int maxAuthBytes() {
        return maxAuthBytes;
    }

    @Override
    public CompletionStage<RoundResult> evaluateRound(byte[] authBytes) {
        try {
            if (saslServer == null) {
                saslServer = Sasl.createSaslServer(
                        OAUTHBEARER_MECHANISM,
                        "kafka",
                        // ^^ align with
                        // o.a.k.common.security.oauthbearer.internals.OAuthBearerSaslServer.OAuthBearerSaslServerFactory#createSaslServer
                        // which uses "kafka", but the kafka internals don't depend on it.
                        null,
                        null,
                        callbackHandler);

                if (saslServer == null) {
                    return CompletableFuture.completedFuture(
                            RoundResult.failure(new byte[0],
                                    new SaslException("Failed to create OAUTHBEARER SASL server")));
                }
            }

            byte[] response = saslServer.evaluateResponse(authBytes);

            if (saslServer.isComplete()) {
                String authorizationId = saslServer.getAuthorizationID();
                Instant sessionExpiry = extractTokenExpiry();
                return CompletableFuture.completedFuture(
                        RoundResult.success(
                                response != null ? response : new byte[0],
                                authorizationId,
                                sessionExpiry));
            }
            else {
                return CompletableFuture.completedFuture(
                        RoundResult.challenge(
                                response != null ? response : new byte[0]));
            }
        }
        catch (Exception e) {
            LOGGER.atDebug()
                    .setCause(e)
                    .addKeyValue("error", e.getMessage())
                    .log("OAUTHBEARER authentication failed");
            return CompletableFuture.completedFuture(
                    RoundResult.failure(new byte[0], e));
        }
    }

    @Nullable
    private Instant extractTokenExpiry() {
        // CREDENTIAL.LIFETIME.MS has a misleading name — it is actually an absolute epoch timestamp
        Object credentialExpiryEpochMs = saslServer.getNegotiatedProperty("CREDENTIAL.LIFETIME.MS");
        if (credentialExpiryEpochMs == null) {
            return null;
        }
        if (credentialExpiryEpochMs instanceof Long expiryEpochMs) {
            return Instant.ofEpochMilli(expiryEpochMs);
        }
        throw new IllegalStateException(
                "Expected CREDENTIAL.LIFETIME.MS to be Long but was " + credentialExpiryEpochMs.getClass().getName());
    }

    @Override
    public void dispose() {
        if (saslServer != null) {
            try {
                saslServer.dispose();
            }
            catch (SaslException e) {
                LOGGER.atDebug()
                        .setCause(e)
                        .log("Error disposing OAUTHBEARER SASL server");
            }
            finally {
                saslServer = null;
            }
        }
    }
}
