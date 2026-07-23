/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.time.Clock;
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
public class OauthBearerHandler implements MechanismHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OauthBearerHandler.class);

    private final OAuthBearerValidatorCallbackHandler callbackHandler;
    private final Clock clock;

    @Nullable
    private SaslServer saslServer;

    /**
     * Create an OAUTHBEARER handler.
     *
     * @param callbackHandler the configured callback handler for JWT validation
     * @param clock clock for computing token remaining lifetime
     */
    public OauthBearerHandler(OAuthBearerValidatorCallbackHandler callbackHandler, Clock clock) {
        this.callbackHandler = Objects.requireNonNull(callbackHandler);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public String mechanismName() {
        return OAUTHBEARER_MECHANISM;
    }

    @Override
    public CompletionStage<AuthenticationResult> handleAuthenticate(byte[] authBytes) {
        try {
            if (saslServer == null) {
                saslServer = Sasl.createSaslServer(
                        OAUTHBEARER_MECHANISM,
                        "kafka",
                        null,
                        null,
                        callbackHandler);

                if (saslServer == null) {
                    return CompletableFuture.completedFuture(
                            AuthenticationResult.failure(new byte[0],
                                    "Failed to create OAUTHBEARER SASL server"));
                }
            }

            byte[] response = saslServer.evaluateResponse(authBytes);

            if (saslServer.isComplete()) {
                String authorizationId = saslServer.getAuthorizationID();
                long sessionLifetimeMs = extractTokenLifetimeMs();
                return CompletableFuture.completedFuture(
                        AuthenticationResult.success(
                                response != null ? response : new byte[0],
                                authorizationId,
                                sessionLifetimeMs));
            }
            else {
                return CompletableFuture.completedFuture(
                        AuthenticationResult.challenge(
                                response != null ? response : new byte[0]));
            }
        }
        catch (Exception e) {
            LOGGER.atDebug()
                    .setCause(e)
                    .log("OAUTHBEARER authentication failed");
            return CompletableFuture.completedFuture(
                    AuthenticationResult.failure(new byte[0],
                            "Authentication failed: " + e.getMessage()));
        }
    }

    private long extractTokenLifetimeMs() {
        try {
            Object lifetimeMs = saslServer.getNegotiatedProperty("CREDENTIAL.LIFETIME.MS");
            if (lifetimeMs instanceof Long tokenExpiryMs) {
                return Math.max(0, tokenExpiryMs - clock.millis());
            }
        }
        catch (Exception e) {
            LOGGER.atDebug()
                    .setCause(e)
                    .log("Could not extract token lifetime");
        }
        return 0;
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
