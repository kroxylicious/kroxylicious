/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.VerificationJwkSelector;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;

import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.filter.validation.validators.Result;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Checks if the {@link Record} headers contains a valid {@link JsonWebSignature} Signature.
 * <p>
 * The JWS Signature is validated using the provided {@link JsonWebKeySet} and {@link AlgorithmConstraints}.
 * </p>
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7515">RFC 7515 (JWS)</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7797">RFC 7797 (JWS Unencoded Payload Option)</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7517">RFC 7517 (JWK)</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7518">RFC 7518 (JWA)</a>
 */
public class JwsSignatureBytebufValidator implements BytebufValidator {
    private static final String DEFAULT_ERROR_MESSAGE = "JWS Signature could not be successfully verified";
    private final String jwsRecordHeaderKey;

    private static final VerificationJwkSelector jwkSelector = new VerificationJwkSelector();

    private final JsonWebSignature jws;
    private final JsonWebKeySet trustedJsonWebKeySet;
    private final boolean isContentDetached;
    private final boolean failOnMissingJwsRecordHeader;

    /**
     * Constructor for {@link JwsSignatureBytebufValidator}.
     *
     * @see <a href="https://bitbucket.org/b_c/jose4j/wiki/JWS%20Examples">jose4j JWS examples</a>
     */
    public JwsSignatureBytebufValidator(JsonWebKeySet trustedJsonWebKeySet, AllowDeny<String> allowedAndDeniedAlgorithms, String jwsRecordHeaderKey, boolean isContentDetached, boolean failOnMissingJwsRecordHeader) {
        this.jws = new JsonWebSignature();
        this.trustedJsonWebKeySet = trustedJsonWebKeySet;
        this.jwsRecordHeaderKey = jwsRecordHeaderKey;
        this.isContentDetached = isContentDetached;
        this.failOnMissingJwsRecordHeader = failOnMissingJwsRecordHeader;

        AlgorithmConstraints algorithmConstraints = extractAlgorithmConstraints(allowedAndDeniedAlgorithms);
        jws.setAlgorithmConstraints(algorithmConstraints);
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        Objects.requireNonNull(record);

        if (record.headers().length == 0 || new RecordHeaders(record.headers()).lastHeader(jwsRecordHeaderKey) == null) {
            if (!failOnMissingJwsRecordHeader) {
                String message = String.format("Returning valid result even though JWS record header is missing (because of config): %s", jwsRecordHeaderKey);
                return CompletableFuture.completedStage(new Result(true, message));
            }

            String message = DEFAULT_ERROR_MESSAGE + ": valid " + jwsRecordHeaderKey + " JWS record header could not be found";
            return CompletableFuture.completedStage(new Result(false, message));
        }

        byte[] jwsHeaderValue = new RecordHeaders(record.headers()).lastHeader(jwsRecordHeaderKey).value();
        String jwsHeaderValueString = new String(jwsHeaderValue, StandardCharsets.UTF_8);

        String payload = null;
        if (isContentDetached) {
            payload = new String(StandardCharsets.UTF_8.decode(buffer).array());
        }

        try {
            if (!isSignatureValid(jwsHeaderValueString, payload)) {
                String message = DEFAULT_ERROR_MESSAGE + ": JWS Signature was invalid";
                return CompletableFuture.completedStage(new Result(false, message));
            }

            return Result.VALID_RESULT_STAGE;
        }
        catch (JoseException e) {
            String message = DEFAULT_ERROR_MESSAGE + ": Jose4j threw an exception: " + (e.getMessage() != null ? e.getMessage() : "(exception message was null)");
            return CompletableFuture.completedStage(new Result(false, message));
        }
    }

    /**
     * Validates a JWS Signature using the provided {@link JsonWebKeySet} and {@link AlgorithmConstraints}.
     *
     * @param jwsCompactSerialization Result of {@link JsonWebSignature#getCompactSerialization()}
     * @param payload The payload of the {@link JsonWebSignature} (only pass this if using {@link JsonWebSignature#getDetachedContentCompactSerialization()})
     * @return True if the signature was validated successfully, otherwise False.
     * @throws JoseException If a {@link JsonWebKey} that matches the {@link AlgorithmConstraints} cannot be found in the {@link JsonWebKeySet}, the {@link JsonWebSignature} cannot be deserialized, etc.
     */
    private boolean isSignatureValid(String jwsCompactSerialization, @Nullable String payload) throws JoseException {
        this.jws.setCompactSerialization(jwsCompactSerialization);

        // Cannot use a function overload because order of execution matters for jose4j jws set functions.
        if (payload != null) {
            this.jws.setPayload(payload);
        }

        // We can only select the key after setCompactSerialization() has set the JWS's "alg" header
        JsonWebKey jwk = jwkSelector.select(this.jws, trustedJsonWebKeySet.getJsonWebKeys());
        if (jwk == null) {
            throw new UnresolvableKeyException("Could not select a valid JWK that matches the algorithm constraints");
        }

        this.jws.setKey(jwk.getKey());

        return this.jws.verifySignature();
    }

    /**
     * Convert an {@link AllowDeny} containing {@link AlgorithmIdentifiers} into {@link AlgorithmConstraints} using the following strategy (null -> empty):
     *
     * <ul>
     *     <li>If both {@link AllowDeny#allowed()} and {@link AllowDeny#denied()} are empty: block all algorithms.</li>
     *     <li>If only {@link AllowDeny#allowed()} is filled: only allow the algorithms in {@link AllowDeny#allowed()}.</li>
     *     <li>If only {@link AllowDeny#denied()} is filled: allow all algorithms except the ones in {@link AllowDeny#denied()}.</li>
     *     <li>If both {@link AllowDeny#allowed()} and {@link AllowDeny#denied()} are filled: only allow the algorithms in {@link AllowDeny#allowed()}.</li>
     * </ul>
     *
     * @param allowedAndDeniedAlgorithms An {@link AllowDeny} containing {@link AlgorithmIdentifiers}.
     * @return The {@link AlgorithmConstraints} equivalent of the passed {@link AllowDeny}.
     */
    private static AlgorithmConstraints extractAlgorithmConstraints(AllowDeny<String> allowedAndDeniedAlgorithms) {
        String[] allowedAlgorithms = Optional.ofNullable(allowedAndDeniedAlgorithms.allowed()).orElse(List.of()).toArray(new String[0]);
        String[] deniedAlgorithms = Optional.ofNullable(allowedAndDeniedAlgorithms.denied()).orElse(Set.of()).toArray(new String[0]);

        AlgorithmConstraints.ConstraintType constraintType = AlgorithmConstraints.ConstraintType.PERMIT;
        String[] algorithms = allowedAlgorithms;

        if (allowedAlgorithms.length == 0 && deniedAlgorithms.length > 0) {
            constraintType = AlgorithmConstraints.ConstraintType.BLOCK;
            algorithms = deniedAlgorithms;
        }

        return new AlgorithmConstraints(constraintType, algorithms);
    }
}
