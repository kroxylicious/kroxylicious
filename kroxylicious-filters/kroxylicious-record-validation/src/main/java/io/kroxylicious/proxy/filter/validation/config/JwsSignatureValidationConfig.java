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

    private final JsonWebKeySet jsonWebKeySet;
    private final AlgorithmConstraints algorithmConstraints;
    private final String jwsHeaderName;
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
    public JwsSignatureValidationConfig(@JsonProperty(value = "jsonWebKeySet", required = true) @JsonDeserialize(using = JsonWebKeySetDeserializer.class) JsonWebKeySet jsonWebKeySet,
                                        @JsonProperty(value = "algorithmConstraintType", defaultValue = "BLOCK") @Nullable AlgorithmConstraints.ConstraintType nullableAlgorithmConstraintType,
                                        @JsonProperty(value = "algorithms", defaultValue = "[]") @Nullable String[] nullableAlgorithms,
                                        @JsonProperty(value = "jwsHeaderName", defaultValue = "jws") @Nullable String nullableJwsHeaderName,
                                        @JsonProperty(value = "isContentDetached", defaultValue = "false") boolean nullableIsContentDetached) {
        this.jsonWebKeySet = jsonWebKeySet;

        AlgorithmConstraints.ConstraintType algorithmConstraintType = nullableAlgorithmConstraintType != null ? nullableAlgorithmConstraintType
                : AlgorithmConstraints.ConstraintType.BLOCK;
        String[] algorithms = nullableAlgorithms != null ? nullableAlgorithms : new String[]{};
        this.algorithmConstraints = new AlgorithmConstraints(algorithmConstraintType, algorithms);
        this.jwsHeaderName = nullableJwsHeaderName != null ? nullableJwsHeaderName : "jws";
        this.isContentDetached = nullableIsContentDetached;
    }

    public JsonWebKeySet getJsonWebKeySet() {
        return jsonWebKeySet;
    }

    public AlgorithmConstraints getAlgorithmConstraints() {
        return algorithmConstraints;
    }

    public String getJwsHeaderName() {
        return jwsHeaderName;
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

        List<JsonWebKey> keyList = jsonWebKeySet.getJsonWebKeys();
        boolean hasSameAmountOfKeys = keyList.size() == that.jsonWebKeySet.getJsonWebKeys().size();
        boolean allKeysFound = keyList.stream()
                .allMatch(key -> that.jsonWebKeySet.findJsonWebKey(key.getKeyId(), key.getKeyType(), key.getUse(), key.getAlgorithm()) != null);

        return hasSameAmountOfKeys && allKeysFound && jwsHeaderName.equals(that.jwsHeaderName) && isContentDetached == that.isContentDetached;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonWebKeySet, algorithmConstraints, jwsHeaderName, isContentDetached);
    }

    @Override
    public String toString() {
        // Probably best to keep this primitive in order to not leak sensitive information
        return "JwsSignatureValidationConfig{" +
                "jsonWebKeySet=" + jsonWebKeySet +
                ", algorithmConstraintType='" + algorithmConstraints + '\'' +
                ", jwsHeaderName='" + jwsHeaderName + '\'' +
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
                String message = "Could not deserialize JsonWebKeySet" + (e.getMessage() != null ? ": " + e.getMessage() : "");
                throw new JsonParseException(message);
            }

            return jwks;
        }
    }
}
