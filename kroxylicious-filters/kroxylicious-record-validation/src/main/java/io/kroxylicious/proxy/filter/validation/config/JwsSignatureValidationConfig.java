/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.InvalidAlgorithmException;
import org.jose4j.lang.JoseException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for validating a {@link org.apache.kafka.common.record.Record} contains a valid {@link org.jose4j.jws.JsonWebSignature} Signature.
 */
@SuppressWarnings("java:S112") // Needed in static block
public class JwsSignatureValidationConfig {
    private static final List<String> algorithmIdentifiersClassValues;

    private final JsonWebKeySet trustedJsonWebKeySet;
    private final AlgorithmConstraints algorithmConstraints;
    private final String jwsRecordHeaderKey;
    private final boolean isContentDetached;

    static {
        @SuppressWarnings("java:S2440") // Needed for reflection
        Object algorithmIdentifiers = new AlgorithmIdentifiers();
        Field[] declaredFields = algorithmIdentifiers.getClass().getDeclaredFields();
        algorithmIdentifiersClassValues = Arrays.stream(declaredFields).map(field -> {
            try {
                return field.get(algorithmIdentifiers).toString();
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }

    /**
     * Construct JwsSignatureValidationConfig
     * @param nullableAlgorithms Array of {@link AlgorithmIdentifiers}.
     */
    @JsonCreator
    public JwsSignatureValidationConfig(@JsonProperty(value = "trustedJsonWebKeySet", required = true) @JsonDeserialize(using = JsonWebKeySetDeserializer.class) JsonWebKeySet trustedJsonWebKeySet,
                                        @JsonProperty(value = "algorithmConstraintType", defaultValue = "BLOCK") @Nullable AlgorithmConstraints.ConstraintType nullableAlgorithmConstraintType,
                                        @JsonProperty(value = "algorithms", defaultValue = "[]") @Nullable String[] nullableAlgorithms,
                                        @JsonProperty(value = "jwsRecordHeaderKey", defaultValue = "kroxylicious.io/jws") @Nullable String nullablejwsRecordHeaderKey,
                                        @JsonProperty(value = "isContentDetached", defaultValue = "false") boolean nullableIsContentDetached) {
        this.trustedJsonWebKeySet = trustedJsonWebKeySet;

        AlgorithmConstraints.ConstraintType algorithmConstraintType = nullableAlgorithmConstraintType != null ? nullableAlgorithmConstraintType
                : AlgorithmConstraints.ConstraintType.BLOCK;
        String[] algorithms = nullableAlgorithms != null ? nullableAlgorithms : new String[]{};
        this.algorithmConstraints = new AlgorithmConstraints(algorithmConstraintType, algorithms);
        this.jwsRecordHeaderKey = nullablejwsRecordHeaderKey != null ? nullablejwsRecordHeaderKey : "kroxylicious.io/jws";
        this.isContentDetached = nullableIsContentDetached;
    }

    public JsonWebKeySet getJsonWebKeySet() {
        return trustedJsonWebKeySet;
    }

    public AlgorithmConstraints getAlgorithmConstraints() {
        return algorithmConstraints;
    }

    public String getjwsRecordHeaderKey() {
        return jwsRecordHeaderKey;
    }

    public boolean getIsContentDetached() {
        return isContentDetached;
    }

    /**
     * Both {@link JsonWebKeySet} and {@link AlgorithmConstraints} use the default {@link Object#equals(Object)} which is insufficient. Instead:
     *
     * <ul>
     * <li>For {@link JsonWebKeySet}, the value of {@link JsonWebKeySet#toJson()} is compared.</li>
     * <li>For {@link AlgorithmConstraints}, the result of {@link AlgorithmConstraints#checkConstraint(String)} is compared for each {@link AlgorithmIdentifiers} property.</li>
     * </ul>
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JwsSignatureValidationConfig that = (JwsSignatureValidationConfig) o;

        // Check all constraints match
        for (String algorithIdentifier : algorithmIdentifiersClassValues) {
            boolean thisThrew = false;
            boolean thatThrew = false;

            try {
                algorithmConstraints.checkConstraint(algorithIdentifier);
            }
            catch (InvalidAlgorithmException e) {
                thisThrew = true;
            }

            try {
                that.algorithmConstraints.checkConstraint(algorithIdentifier);
            }
            catch (InvalidAlgorithmException e) {
                thatThrew = true;
            }

            if (thisThrew != thatThrew) {
                return false;
            }
        }

        List<JsonWebKey> keyList = trustedJsonWebKeySet.getJsonWebKeys();
        boolean hasSameAmountOfKeys = keyList.size() == that.trustedJsonWebKeySet.getJsonWebKeys().size();
        boolean allKeysFound = keyList.stream()
                .allMatch(key -> that.trustedJsonWebKeySet.findJsonWebKey(key.getKeyId(), key.getKeyType(), key.getUse(), key.getAlgorithm()) != null);

        return hasSameAmountOfKeys && allKeysFound && jwsRecordHeaderKey.equals(that.jwsRecordHeaderKey) && isContentDetached == that.isContentDetached;
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustedJsonWebKeySet, algorithmConstraints, jwsRecordHeaderKey, isContentDetached);
    }

    @Override
    public String toString() {
        // Probably best to keep this primitive in order to not leak sensitive information
        return "JwsSignatureValidationConfig{" +
                "trustedJsonWebKeySet=" + trustedJsonWebKeySet +
                ", algorithmConstraintType='" + algorithmConstraints + '\'' +
                ", jwsRecordHeaderKey='" + jwsRecordHeaderKey + '\'' +
                ", isContentDetached='" + isContentDetached + '\'' +
                '}';
    }

    public static class JsonWebKeySetDeserializer extends StdDeserializer<JsonWebKeySet> {
        public JsonWebKeySetDeserializer() {
            this(null);
        }

        JsonWebKeySetDeserializer(@Nullable Class<?> vc) {
            super(vc);
        }

        @Override
        public JsonWebKeySet deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);

            JsonWebKeySet jwks;

            try {
                // WARNING: We're using arbitrary json from the user here. This may be susceptible to an injection attack?
                jwks = new JsonWebKeySet(node.textValue());
            }
            catch (JoseException e) {
                String message = "Could not deserialize TrustedJsonWebKeySet" + (e.getMessage() != null ? ": " + e.getMessage() : "");
                throw new JsonParseException(message);
            }

            return jwks;
        }
    }
}
