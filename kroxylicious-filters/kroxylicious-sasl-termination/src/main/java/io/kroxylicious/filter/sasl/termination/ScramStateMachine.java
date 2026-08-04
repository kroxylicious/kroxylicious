/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
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

import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>Handles SCRAM authentication for SHA-256 and SHA-512.</p>
 * <p>
 * This state machine uses Kafka's {@link SaslServer} implementation to process
 * the SCRAM protocol exchange. It asynchronously fetches credentials from
 * the credential store on first use, then handles subsequent rounds synchronously.
 * </p>
 */
class ScramStateMachine implements MechanismStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScramStateMachine.class);

    static final int MAX_USERNAME_LENGTH = 255;
    private static final int PHANTOM_SALT_LENGTH = 20;
    private static final int PHANTOM_ITERATIONS = 10000;
    private static final int PHANTON_NUM_SERVER_NONCE_BYTES = 24;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String LOG_KEY_USERNAME = "username";

    private final String mechanismName;
    private final ScramCredentialStore credentialStore;

    // Set during the first round when the credential lookup finds the user
    @Nullable
    private SaslServer saslServer;

    // Set during the first round by parsing the client-first-message
    @Nullable
    private String extractedUsername;

    // Set during the first round when the credential lookup does not find the user
    private boolean phantomUser;

    /**
     * Create a SCRAM state machine for the specified mechanism.
     *
     * @param mechanism the SCRAM mechanism (SHA-256 or SHA-512)
     * @param credentialStore the credential store for looking up user credentials
     */
    ScramStateMachine(ScramMechanism mechanism, ScramCredentialStore credentialStore) {
        this.mechanismName = mechanism.mechanismName();
        this.credentialStore = Objects.requireNonNull(credentialStore);
    }

    @Override
    public String mechanismName() {
        return mechanismName;
    }

    @Override
    public int maxAuthBytes() {
        return 4 * 1024;
    }

    @Override
    public CompletionStage<RoundResult> evaluateRound(byte[] authBytes) {
        if (saslServer == null && !phantomUser) {
            return handleFirstRound(authBytes);
        }
        else {
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
                LOGGER.atDebug()
                        .setCause(e)
                        .log("Error disposing {} SASL server", mechanismName());
            }
            finally {
                saslServer = null;
            }
        }
    }

    private CompletionStage<RoundResult> handleFirstRound(byte[] authBytes) {
        try {
            extractedUsername = extractUsername(authBytes);

            return credentialStore.lookupCredential(extractedUsername)
                    .thenCompose(credential -> {
                        if (credential == null) {
                            return generatePhantomChallenge(authBytes);
                        }
                        return processWithCredential(authBytes, credential);
                    })
                    .exceptionally(throwable -> {
                        Throwable cause = throwable instanceof java.util.concurrent.CompletionException ce ? ce.getCause() : throwable;
                        Exception exception = cause instanceof Exception e ? e : new RuntimeException(cause);
                        return RoundResult.failure(new byte[0], exception);
                    });
        }
        catch (Exception e) {
            return CompletableFuture.completedFuture(
                    RoundResult.failure(new byte[0], e));
        }
    }

    private CompletionStage<RoundResult> processWithCredential(
                                                               byte[] authBytes,
                                                               ScramCredential credential) {
        try {
            CallbackHandler callbackHandler = callbacks -> {
                for (Callback callback : callbacks) {
                    switch (callback) {
                        case NameCallback nameCallback -> nameCallback.setName(extractedUsername);
                        case ScramCredentialCallback scramCallback -> scramCallback.scramCredential(convertCredential(credential));
                        default -> throw new UnsupportedCallbackException(callback);
                    }
                }
            };

            saslServer = Sasl.createSaslServer(
                    mechanismName,
                    "kafka",
                    null,
                    Map.<String, String> of(),
                    callbackHandler);

            if (saslServer == null) {
                return CompletableFuture.completedFuture(
                        RoundResult.failure(new byte[0], new SaslException("Failed to create SASL server")));
            }

            return evaluateResponse(authBytes);
        }
        catch (Exception e) {
            return CompletableFuture.completedFuture(
                    RoundResult.failure(new byte[0], e));
        }
    }

    private CompletionStage<RoundResult> handleSubsequentRound(byte[] authBytes) {
        if (phantomUser) {
            LOGGER.atDebug()
                    .addKeyValue(LOG_KEY_USERNAME, extractedUsername)
                    .log("Completing phantom user authentication with failure");
            return CompletableFuture.completedFuture(
                    RoundResult.failure(new byte[0], new SaslException("Authentication failed")));
        }
        return evaluateResponse(authBytes);
    }

    private CompletionStage<RoundResult> evaluateResponse(byte[] authBytes) {
        try {
            byte[] response = Objects.requireNonNull(saslServer).evaluateResponse(authBytes);

            if (saslServer.isComplete()) {
                String authorizationId = saslServer.getAuthorizationID();
                return CompletableFuture.completedFuture(
                        RoundResult.success(response, authorizationId));
            }
            else {
                return CompletableFuture.completedFuture(
                        RoundResult.challenge(response));
            }
        }
        catch (SaslException e) {
            LOGGER.atError()
                    .addKeyValue(LOG_KEY_USERNAME, extractedUsername)
                    .setCause(e)
                    .log("Could not evaluate a SASL response");
            return CompletableFuture.completedFuture(
                    RoundResult.failure(new byte[0], e));
        }
    }

    private CompletionStage<RoundResult> generatePhantomChallenge(byte[] clientFirstMessage) {
        phantomUser = true;
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_USERNAME, extractedUsername)
                .log("User not found in credential store, generating phantom challenge");

        String clientNonce = extractClientNonce(clientFirstMessage);

        // Deterministic salt derived from username so repeated probes get the same salt
        byte[] salt;
        try {
            String algorithm = mechanismName.contains("512") ? "SHA-512" : "SHA-256";
            MessageDigest md = MessageDigest.getInstance(algorithm);
            salt = Arrays.copyOf(md.digest(Objects.requireNonNull(extractedUsername).getBytes(StandardCharsets.UTF_8)), PHANTOM_SALT_LENGTH);
        }
        catch (NoSuchAlgorithmException e) {
            salt = new byte[PHANTOM_SALT_LENGTH];
            SECURE_RANDOM.nextBytes(salt);
        }

        byte[] serverNonceBytes = new byte[PHANTON_NUM_SERVER_NONCE_BYTES];
        SECURE_RANDOM.nextBytes(serverNonceBytes);
        String serverNonce = Base64.getEncoder().encodeToString(serverNonceBytes);

        String serverFirstMessage = "r=" + clientNonce + serverNonce
                + ",s=" + Base64.getEncoder().encodeToString(salt)
                + ",i=" + PHANTOM_ITERATIONS;

        return CompletableFuture.completedFuture(
                RoundResult.challenge(serverFirstMessage.getBytes(StandardCharsets.UTF_8)));
    }

    private static String extractClientNonce(byte[] clientFirstMessage) {
        String message = new String(clientFirstMessage, StandardCharsets.UTF_8);
        int nonceStart = message.indexOf("r=");
        if (nonceStart == -1) {
            throw new IllegalArgumentException("Invalid SCRAM message: no nonce field");
        }
        nonceStart += 2;
        int nonceEnd = message.indexOf(',', nonceStart);
        return nonceEnd == -1 ? message.substring(nonceStart) : message.substring(nonceStart, nonceEnd);
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
        if (username.length() > MAX_USERNAME_LENGTH) {
            throw new IllegalArgumentException("Invalid SCRAM message: username exceeds maximum length of " + MAX_USERNAME_LENGTH + " characters");
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
