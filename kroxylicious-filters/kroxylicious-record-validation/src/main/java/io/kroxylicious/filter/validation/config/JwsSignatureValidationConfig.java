/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.JoseException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import io.kroxylicious.proxy.config.tls.AllowDeny;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for validating a {@link org.apache.kafka.common.record.Record} contains a valid {@link org.jose4j.jws.JsonWebSignature} Signature.
 */
public class JwsSignatureValidationConfig {
    private final JsonWebKeySet trustedJsonWebKeySet;
    private final AllowDeny<String> allowedAndDeniedAlgorithms;
    private final String jwsRecordHeaderKey;
    private final Boolean isContentDetached;
    private final Boolean failOnMissingJwsRecordHeader;

    /**
     * Construct JwsSignatureValidationConfig
     * @param nullableAlgorithms Array of {@link AlgorithmIdentifiers}.
     */
    @JsonCreator
    public JwsSignatureValidationConfig(@JsonProperty(value = "trustedJsonWebKeySet", required = true) @JsonDeserialize(using = JsonWebKeySetDeserializer.class) JsonWebKeySet trustedJsonWebKeySet,
                                        @JsonProperty(value = "algorithms") @Nullable AllowDeny<String> nullableAlgorithms,
                                        @JsonProperty(value = "jwsRecordHeaderKey", defaultValue = "kroxylicious.io/jws") @Nullable String nullablejwsRecordHeaderKey,
                                        @JsonProperty(value = "contentDetached", defaultValue = "false") @Nullable Boolean nullableIsContentDetached,
                                        @JsonProperty(value = "failOnMissingJwsRecordHeader", defaultValue = "true") @Nullable Boolean nullableFailOnMissingJwsRecordHeader) {
        this.trustedJsonWebKeySet = trustedJsonWebKeySet;

        this.allowedAndDeniedAlgorithms = nullableAlgorithms != null ? nullableAlgorithms : new AllowDeny<>(List.of(), Set.of());

        this.jwsRecordHeaderKey = nullablejwsRecordHeaderKey != null ? nullablejwsRecordHeaderKey : "kroxylicious.io/jws";
        this.isContentDetached = nullableIsContentDetached != null && nullableIsContentDetached;
        this.failOnMissingJwsRecordHeader = nullableFailOnMissingJwsRecordHeader == null || nullableFailOnMissingJwsRecordHeader;
    }

    public JsonWebKeySet getJsonWebKeySet() {
        return trustedJsonWebKeySet;
    }

    public AllowDeny<String> getAllowedAndDeniedAlgorithms() {
        return allowedAndDeniedAlgorithms;
    }

    public String getjwsRecordHeaderKey() {
        return jwsRecordHeaderKey;
    }

    public boolean getIsContentDetached() {
        return isContentDetached;
    }

    public boolean getFailOnMissingJwsRecordHeader() {
        return failOnMissingJwsRecordHeader;
    }

    /**
     * Both {@link JsonWebKeySet} and {@link AllowDeny} use the default {@link Object#equals(Object)} which is insufficient. Instead:
     *
     * <ul>
     * <li>For {@link JsonWebKeySet}, the value of {@link JsonWebKeySet#toJson()} is compared.</li>
     * <li>For {@link AllowDeny}, {@link List#equals(Object)} and {@link Set#equals(Object)} are used to compare the values of
     * {@link AllowDeny#allowed()} (after sorting) and {@link AllowDeny#denied()} respectively.</li>
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

        ArrayList<String> thisAllowedAlgorithms = new ArrayList<>(Optional.ofNullable(allowedAndDeniedAlgorithms.allowed()).orElse(List.of()));
        thisAllowedAlgorithms.sort(null);
        Set<String> thisDeniedAlgorithms = Optional.ofNullable(allowedAndDeniedAlgorithms.denied()).orElse(Set.of());

        ArrayList<String> thatAllowedAlgorithms = new ArrayList<>(Optional.ofNullable(that.allowedAndDeniedAlgorithms.allowed()).orElse(List.of()));
        thatAllowedAlgorithms.sort(null);
        Set<String> thatDeniedAlgorithms = Optional.ofNullable(that.allowedAndDeniedAlgorithms.denied()).orElse(Set.of());

        if (!thisDeniedAlgorithms.equals(thatDeniedAlgorithms) || !thisAllowedAlgorithms.equals(thatAllowedAlgorithms)) {
            return false;
        }

        List<JsonWebKey> keyList = trustedJsonWebKeySet.getJsonWebKeys();
        boolean hasSameAmountOfKeys = keyList.size() == that.trustedJsonWebKeySet.getJsonWebKeys().size();
        boolean allKeysFound = keyList.stream()
                .allMatch(key -> that.trustedJsonWebKeySet.findJsonWebKey(key.getKeyId(), key.getKeyType(), key.getUse(), key.getAlgorithm()) != null);

        return hasSameAmountOfKeys && allKeysFound && jwsRecordHeaderKey.equals(that.jwsRecordHeaderKey) && isContentDetached == that.isContentDetached
                && failOnMissingJwsRecordHeader == that.failOnMissingJwsRecordHeader;
    }

    @Override
    public int hashCode() {
        List<String> allowedAlgorithms = new ArrayList<>(Optional.ofNullable(allowedAndDeniedAlgorithms.allowed()).orElse(List.of()));
        allowedAlgorithms.sort(null);

        Set<String> deniedAlgorithms = Optional.ofNullable(allowedAndDeniedAlgorithms.denied()).orElse(Set.of());

        List<String> jsonWebKeys = trustedJsonWebKeySet.getJsonWebKeys().stream()
                .map(k -> String.join("|",
                        k.getKeyId(),
                        k.getKeyType(),
                        k.getUse(),
                        k.getAlgorithm()))
                .sorted()
                .toList();

        return Objects.hash(
                jsonWebKeys,
                allowedAlgorithms,
                deniedAlgorithms,
                jwsRecordHeaderKey,
                isContentDetached,
                failOnMissingJwsRecordHeader);
    }

    @Override
    public String toString() {
        // Probably best to keep this primitive in order to not leak sensitive information
        return "JwsSignatureValidationConfig{" +
                "trustedJsonWebKeySet=" + trustedJsonWebKeySet +
                ", allowedAndDeniedAlgorithms='" + allowedAndDeniedAlgorithms + '\'' +
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
