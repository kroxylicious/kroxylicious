/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
 * Handles SCRAM-SHA-256 authentication.
 * <p>
 * This handler uses Kafka's {@link SaslServer} implementation to process
 * the SCRAM protocol exchange. It asynchronously fetches credentials from
 * the credential store on first use, then handles subsequent rounds synchronously.
 * </p>
 *
 * <h2>Multi-Round Authentication</h2>
 * <p>
 * SCRAM-SHA-256 typically requires 3 rounds:
 * </p>
 * <ol>
 *     <li>Client sends first message with username</li>
 *     <li>Server responds with challenge (salt, iterations)</li>
 *     <li>Client sends proof, server responds with verification</li>
 * </ol>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Not thread-safe. Each instance is used for a single connection
 * on that connection's event loop thread.
 * </p>
 */
public class ScramSha256Handler implements MechanismHandler {

    Logger LOGGER = LoggerFactory.getLogger(ScramSha256Handler.class);
    private static final String MECHANISM_NAME = ScramMechanism.SCRAM_SHA_256.mechanismName();
    private static final Map<String, String> SASL_PROPS = Map.of();

    @Nullable
    private SaslServer saslServer;

    @Nullable
    private String extractedUsername;

    @Override
    public String mechanismName() {
        return MECHANISM_NAME;
    }

    @Override
    public CompletionStage<AuthenticationResult> handleAuthenticate(
                                                                    byte[] authBytes,
                                                                    ScramCredentialStore credentialStore) {
        if (saslServer == null) {
            // First round: extract username and fetch credentials asynchronously
            return handleFirstRound(authBytes, credentialStore);
        }
        else {
            // Subsequent rounds: use existing SaslServer synchronously
            return handleSubsequentRound(authBytes);
        }
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

    private CompletionStage<AuthenticationResult> handleFirstRound(
                                                                   byte[] authBytes,
                                                                   ScramCredentialStore credentialStore) {
        try {
            // Extract username from first SCRAM client message
            // Format: n,,n=username,r=client-nonce
            extractedUsername = extractUsername(authBytes);

            // Asynchronously fetch credentials
            return credentialStore.lookupCredential(extractedUsername)
                    .thenCompose(credential -> {
                        if (credential == null) {
                            // Use generic error to prevent username enumeration
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
            // Create callback handler that supplies the credential
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

            // Create SaslServer
            saslServer = Sasl.createSaslServer(
                    MECHANISM_NAME,
                    "kafka",
                    null,
                    SASL_PROPS,
                    callbackHandler);

            if (saslServer == null) {
                return CompletableFuture.completedFuture(
                        AuthenticationResult.failure(new byte[0], "Failed to create SASL server"));
            }

            // Process the first message
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
            LOGGER.atError().setMessage("Could not evaluate a SASL response username={}")
                    .setCause(e)
                    .addArgument(extractedUsername)
                    .log();
            return CompletableFuture.completedFuture(
                    AuthenticationResult.failure(new byte[0], "Authentication failed: " + e.getMessage()));
        }
    }

    /**
     * Extract username from SCRAM client-first-message.
     * <p>
     * Format: {@code n,,n=username,r=client-nonce}
     * </p>
     *
     * @param clientFirstMessage the first message from the client
     * @return the extracted username
     * @throws IllegalArgumentException if the message format is invalid
     */
    private static String extractUsername(byte[] clientFirstMessage) {
        String message = new String(clientFirstMessage, StandardCharsets.UTF_8);

        // SCRAM client-first-message format: n,,n=username,r=nonce
        // We need to extract the username
        int usernameStart = message.indexOf("n=");
        if (usernameStart == -1) {
            throw new IllegalArgumentException("Invalid SCRAM message: no username field");
        }

        usernameStart += 2; // Skip "n="
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

    /**
     * Convert our ScramCredential to Kafka's ScramCredential format.
     * <p>
     * Note: Kafka's constructor parameter order is (salt, storedKey, serverKey, iterations).
     * </p>
     */
    private static org.apache.kafka.common.security.scram.ScramCredential convertCredential(
                                                                                            ScramCredential credential) {
        return new org.apache.kafka.common.security.scram.ScramCredential(
                credential.salt(),
                credential.storedKey(),
                credential.serverKey(),
                credential.iterations());
    }
}
