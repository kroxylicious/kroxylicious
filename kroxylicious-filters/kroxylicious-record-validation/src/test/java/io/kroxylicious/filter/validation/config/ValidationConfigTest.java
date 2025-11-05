/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.config;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.proxy.filter.validation.validators.bytebuf.JwsBytebufValidatorTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidationConfigTest {
    private static final JsonWebKeySet ECDSA_JWKS = JwsBytebufValidatorTest.ECDSA_JWKS;
    private static final JsonWebKeySet RSA_AND_ECDSA_JWKS = JwsBytebufValidatorTest.RSA_AND_ECDSA_JWKS;

    private static ValidationConfig expectedApicurioConfig() throws MalformedURLException {
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), new SchemaValidationConfig(URI.create("http://localhost:8080").toURL(), 1L), false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, false, true), null);
        return new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, false, true)));
    }

    public static Stream<Arguments> deserialize() throws MalformedURLException {
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, true, false), null);
        ValidationConfig defaultConfig = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, true, false)));

        TopicMatchingRecordValidationRule nonDefaultRuleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, false, true));
        TopicMatchingRecordValidationRule nonDefaultRuleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, false, true), null);
        ValidationConfig nonDefaultConfig = new ValidationConfig(List.of(nonDefaultRuleOne, nonDefaultRuleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, false, true)));
        return Stream.of(Arguments.argumentSet("default values", """
                defaultRule:
                  valueRule: {}
                rules:
                - topicNames:
                  - one
                  valueRule:
                    syntacticallyCorrectJson: {}
                - topicNames:
                  - two
                  keyRule: {}
                """, defaultConfig),
                Arguments.argumentSet("non default values", """
                        defaultRule:
                          valueRule:
                            allowNulls: false
                            allowEmpty: true
                        rules:
                        - topicNames:
                          - one
                          valueRule:
                            syntacticallyCorrectJson:
                                validateObjectKeysUnique: true
                            allowNulls: false
                            allowEmpty: true
                        - topicNames:
                          - two
                          keyRule:
                            allowNulls: false
                            allowEmpty: true
                        """, nonDefaultConfig),
                Arguments.argumentSet("non default apicurio values", """
                        defaultRule:
                          valueRule:
                            allowNulls: false
                            allowEmpty: true
                        rules:
                        - topicNames:
                          - one
                          valueRule:
                            syntacticallyCorrectJson:
                                validateObjectKeysUnique: true
                            schemaValidationConfig:
                                apicurioGlobalId: 1
                                apicurioRegistryUrl: http://localhost:8080
                            allowNulls: false
                            allowEmpty: true
                        - topicNames:
                          - two
                          keyRule:
                            allowNulls: false
                            allowEmpty: true
                        """, expectedApicurioConfig()));
    }

    @MethodSource
    @ParameterizedTest
    void deserialize(String content, ValidationConfig expected) throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue(content);
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeDefaultValuesJwsValidation() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
                defaultRule:
                  valueRule: {}
                rules:
                - topicNames:
                  - one
                  valueRule:
                    syntacticallyCorrectJson: {}
                    jwsValidationConfig:
                        jsonWebKeySet: >
                            {
                              "keys": [
                                {
                                  "kty": "EC",
                                  "kid": "Trusted ECDSA JWK",
                                  "use": "sig",
                                  "alg": "ES256",
                                  "x": "qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I",
                                  "y": "wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc",
                                  "crv": "P-256"
                                }
                              ]
                            }
                - topicNames:
                  - two
                  keyRule: {}
                """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, new JwsValidationConfig(ECDSA_JWKS, null, null), true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, true, false), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, null, true, false)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeNonDefaultValuesJwsValidation() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue(
                """
                        defaultRule:
                          valueRule:
                            allowNulls: false
                            allowEmpty: true
                        rules:
                        - topicNames:
                          - one
                          valueRule:
                            syntacticallyCorrectJson:
                                validateObjectKeysUnique: true
                            jwsValidationConfig:
                                jsonWebKeySet: >
                                    {
                                      "keys": [
                                          {
                                            "kty": "RSA",
                                            "kid": "Trusted RSA JWK",
                                            "use": "sig",
                                            "alg": "RS256",
                                            "n": "lx-5h_lkVJGc8-NxgKGkfxobBlM7DDCjqAloEieCf8c9KbPJ12V0Bm8arOX-lm0wRSLxmWzQ3NTM6S9pDbSc7fQt77THlMD1zEDMwIAJxfoMt3U_VrMHVdiOvO6YpU3jWBp7BfQNYHJjCSiaxIHqDPuDCh2GTflkU32Nfim7W3iLBuH4_K8ADYHz3QdeX29vzl49PDbUiiJEsjnEconsfuYjrFaN3uGn41mGzjedc7oxlcID8fDxq5bvZvOFBAIqdrxs5qPYw1QlVupB3LT0AMU1qKWOB_vkM1n54eDIxP9Xd__WnvX4wagYqA1OZ9LPBenhBX7_Rbnrap2QNQ58-Q",
                                            "e": "AQAB"
                                          },
                                          {
                                            "kty": "EC",
                                            "kid": "Trusted ECDSA JWK",
                                            "use": "sig",
                                            "alg": "ES256",
                                            "x": "qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I",
                                            "y": "wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc",
                                            "crv": "P-256"
                                          }
                                      ]
                                    }
                                algorithmConstraintType: PERMIT
                                algorithms:
                                    - ES256
                                    - RS256
                            allowNulls: false
                            allowEmpty: true
                        - topicNames:
                          - two
                          keyRule:
                            allowNulls: false
                            allowEmpty: true
                        """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null,
                        new JwsValidationConfig(RSA_AND_ECDSA_JWKS, AlgorithmConstraints.ConstraintType.PERMIT,
                                new String[]{ AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, AlgorithmIdentifiers.RSA_USING_SHA256 }),
                        false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
        assertEquals(expected, deserialised);
    }

}
