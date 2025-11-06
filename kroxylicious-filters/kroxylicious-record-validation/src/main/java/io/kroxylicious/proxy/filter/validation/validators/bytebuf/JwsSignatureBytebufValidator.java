/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.VerificationJwkSelector;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;

import io.kroxylicious.proxy.filter.validation.validators.Result;

/**
 * Checks if the {@link ByteBuffer} contains a valid {@link JsonWebSignature} Signature.
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
    private static final String DEFAULT_ERROR_MESSAGE = "Buffer could not be successfully verified using JWS signature";

    private static final VerificationJwkSelector jwkSelector = new VerificationJwkSelector();

    private final JsonWebSignature jws = new JsonWebSignature();
    private final JsonWebKeySet jsonWebKeySet;

    /**
     * Constructor for {@link JwsSignatureBytebufValidator}.
     *
     * @see <a href="https://bitbucket.org/b_c/jose4j/wiki/JWS%20Examples">jose4j JWS examples</a>
     */
    public JwsSignatureBytebufValidator(JsonWebKeySet jsonWebKeySet, AlgorithmConstraints algorithmConstraints) {
        this.jsonWebKeySet = jsonWebKeySet;
        jws.setAlgorithmConstraints(algorithmConstraints);
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        Objects.requireNonNull(record);
        if (buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        if (buffer.remaining() < 1) {
            throw new IllegalArgumentException("size is less than 1");
        }

        try {
            String bufferString = new String(StandardCharsets.UTF_8.decode(buffer).array());

            if (verifySignature(bufferString)) {
                return Result.VALID_RESULT_STAGE;
            }
        }
        catch (JoseException e) {
            String message = DEFAULT_ERROR_MESSAGE + (e.getMessage() != null ? ": " + e.getMessage() : "");
            return CompletableFuture.completedStage(new Result(false, message));
        }

        return CompletableFuture.completedStage(new Result(false, DEFAULT_ERROR_MESSAGE));
    }

    /**
     * Validates the JWS Signature using the provided {@link JsonWebKeySet} and {@link AlgorithmConstraints}.
     *
     * @param bufferString The {@link ByteBuffer} decoded to a UTF-8 String e.g. using {@link StandardCharsets#UTF_8}'s {@link java.nio.charset.Charset#decode(ByteBuffer)} method.
     * @return True if the signature was validated successfully, otherwise False.
     * @throws JoseException If a {@link JsonWebKey} that matches the {@link AlgorithmConstraints} cannot be found in the {@link JsonWebKeySet}, the {@link JsonWebSignature} cannot be deserialized, etc.
     */
    private boolean verifySignature(String bufferString) throws JoseException {
        jws.setCompactSerialization(bufferString);

        // We can only select the key after setCompactSerialization() has set the JWS's "alg" header
        JsonWebKey jwk = jwkSelector.select(jws, jsonWebKeySet.getJsonWebKeys());
        if (jwk == null) {
            throw new UnresolvableKeyException("Could not select valid JWK that matches the algorithm constraints");
        }

        jws.setKey(jwk.getKey());

        return jws.verifySignature();
    }
}
