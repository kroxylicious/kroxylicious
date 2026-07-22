/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.security.scram.ScramCredentialCallback;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.sasl.credentialstore.CredentialLookupException;
import io.kroxylicious.sasl.credentialstore.ScramCredential;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Handles SCRAM authentication for SHA-256 and SHA-512.
 * <p>
 * This handler uses Kafka's {@link SaslServer} implementation to process
 * the SCRAM protocol exchange. It asynchronously fetches credentials from
 * the credential store on first use, then handles subsequent rounds synchronously.
 * </p>
 * <p>
 * A fixed delay is applied to all authentication rounds to prevent timing
 * side-channels that could be used for username enumeration.
 * </p>
 */
public class ScramHandler implements MechanismHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScramHandler.class);
    private static final Map<String, String> SASL_PROPS = Map.of();
    static final Duration AUTHENTICATION_DELAY = Duration.ofMillis(200);

    private final String mechanismName;
    private final ScramCredentialStore credentialStore;
    private final Clock clock;

    @Nullable
    private SaslServer saslServer;

    @Nullable
    private String extractedUsername;

    /**
     * Create a SCRAM handler for the specified mechanism.
     *
     * @param mechanism the SCRAM mechanism (SHA-256 or SHA-512)
     * @param credentialStore the credential store for looking up user credentials
     * @param clock clock for timing delay computation
     */
    public ScramHandler(ScramMechanism mechanism, ScramCredentialStore credentialStore, Clock clock) {
        this.mechanismName = mechanism.mechanismName();
        this.credentialStore = Objects.requireNonNull(credentialStore);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public String mechanismName() {
        return mechanismName;
    }

    @Override
    public CompletionStage<AuthenticationResult> handleAuthenticate(byte[] authBytes) {
        Instant deadline = clock.instant().plus(AUTHENTICATION_DELAY);
        CompletionStage<AuthenticationResult> result;
        if (saslServer == null) {
            result = handleFirstRound(authBytes);
        }
        else {
            result = handleSubsequentRound(authBytes);
        }
        return result.thenCompose(r -> delayUntil(deadline, r));
    }

    @Override
    public void dispose() {
        if (saslServer != null) {
            try {
                saslServer.dispose();
            }
            catch (SaslException e) {
                // Log but don't throw - dispose must be idempotent and safe
            }
            finally {
                saslServer = null;
            }
        }
    }

    private CompletionStage<AuthenticationResult> handleFirstRound(byte[] authBytes) {
        try {
            extractedUsername = extractUsername(authBytes);

            return credentialStore.lookupCredential(extractedUsername)
                    .thenCompose(credential -> {
                        if (credential == null) {
                            return CompletableFuture.completedFuture(
                                    AuthenticationResult.failure(new byte[0], "Authentication failed"));
                        }
                        return processWithCredential(authBytes, credential);
                    })
                    .exceptionally(throwable -> {
                        String errorMessage = throwable instanceof CredentialLookupException
                                ? throwable.getMessage()
                                : "Credential lookup failed: " + throwable.getMessage();
                        return AuthenticationResult.failure(new byte[0], errorMessage);
                    });
        }
        catch (Exception e) {
            return CompletableFuture.completedFuture(
                    AuthenticationResult.failure(new byte[0], "Invalid SCRAM message: " + e.getMessage()));
        }
    }

    private CompletionStage<AuthenticationResult> processWithCredential(
                                                                        byte[] authBytes,
                                                                        ScramCredential credential) {
        try {
            CallbackHandler callbackHandler = callbacks -> {
                for (Callback callback : callbacks) {
                    if (callback instanceof NameCallback nameCallback) {
                        nameCallback.setName(extractedUsername);
                    }
                    else if (callback instanceof ScramCredentialCallback scramCallback) {
                        scramCallback.scramCredential(convertCredential(credential));
                    }
                    else {
                        throw new UnsupportedCallbackException(callback);
                    }
                }
            };

            saslServer = Sasl.createSaslServer(
                    mechanismName,
                    "kafka",
                    null,
                    SASL_PROPS,
                    callbackHandler);

            if (saslServer == null) {
                return CompletableFuture.completedFuture(
                        AuthenticationResult.failure(new byte[0], "Failed to create SASL server"));
            }

            return evaluateResponse(authBytes);
        }
        catch (Exception e) {
            return CompletableFuture.completedFuture(
                    AuthenticationResult.failure(new byte[0], "SASL server creation failed: " + e.getMessage()));
        }
    }

    private CompletionStage<AuthenticationResult> handleSubsequentRound(byte[] authBytes) {
        return evaluateResponse(authBytes);
    }

    private CompletionStage<AuthenticationResult> evaluateResponse(byte[] authBytes) {
        try {
            byte[] response = saslServer.evaluateResponse(authBytes);

            if (saslServer.isComplete()) {
                String authorizationId = saslServer.getAuthorizationID();
                return CompletableFuture.completedFuture(
                        AuthenticationResult.success(response, authorizationId));
            }
            else {
                return CompletableFuture.completedFuture(
                        AuthenticationResult.challenge(response));
            }
        }
        catch (SaslException e) {
            LOGGER.atError()
                    .addKeyValue("username", extractedUsername)
                    .setCause(e)
                    .log("Could not evaluate a SASL response");
            return CompletableFuture.completedFuture(
                    AuthenticationResult.failure(new byte[0], "Authentication failed: " + e.getMessage()));
        }
    }

    private CompletionStage<AuthenticationResult> delayUntil(Instant deadline, AuthenticationResult result) {
        long remainingMs = Duration.between(clock.instant(), deadline).toMillis();
        if (remainingMs <= 0) {
            return CompletableFuture.completedFuture(result);
        }
        Executor delayed = CompletableFuture.delayedExecutor(remainingMs, TimeUnit.MILLISECONDS);
        return CompletableFuture.supplyAsync(() -> result, delayed);
    }

    private static String extractUsername(byte[] clientFirstMessage) {
        String message = new String(clientFirstMessage, StandardCharsets.UTF_8);

        int usernameStart = message.indexOf("n=");
        if (usernameStart == -1) {
            throw new IllegalArgumentException("Invalid SCRAM message: no username field");
        }

        usernameStart += 2;
        int usernameEnd = message.indexOf(',', usernameStart);
        if (usernameEnd == -1) {
            throw new IllegalArgumentException("Invalid SCRAM message: malformed username field");
        }

        String username = message.substring(usernameStart, usernameEnd);
        if (username.isEmpty()) {
            throw new IllegalArgumentException("Invalid SCRAM message: empty username");
        }

        return username;
    }

    private static org.apache.kafka.common.security.scram.ScramCredential convertCredential(
                                                                                            ScramCredential credential) {
        return new org.apache.kafka.common.security.scram.ScramCredential(
                credential.salt(),
                credential.storedKey(),
                credential.serverKey(),
                credential.iterations());
    }
}
