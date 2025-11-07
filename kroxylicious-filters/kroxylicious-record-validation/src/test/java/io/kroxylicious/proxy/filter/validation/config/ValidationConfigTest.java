/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.proxy.filter.validation.validators.bytebuf.JwsSignatureBytebufValidatorTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidationConfigTest {

    private static final JsonWebKeySet ECDSA_JWKS = JwsSignatureBytebufValidatorTest.ECDSA_JWKS;
    private static final JsonWebKeySet RSA_AND_ECDSA_JWKS = JwsSignatureBytebufValidatorTest.RSA_AND_ECDSA_JWKS;

    @Test
    void testDecodeDefaultValues() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
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
                """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, null, true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, true, false), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, null, true, false)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeNonDefaultValues() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
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
                """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, null, false, true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeDefaultValuesSchemaValidation() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
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
                """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, null, true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, true, false), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, null, true, false)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeNonDefaultValuesSchemaValidation() throws JsonProcessingException, MalformedURLException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
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
                """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), new SchemaValidationConfig(URI.create("http://localhost:8080").toURL(), 1L), null, false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeInvalidValuesSchemaValidation() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
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
                """);

        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, null, false, true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeDefaultValuesJwsSignatureValidation() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
                defaultRule:
                  valueRule: {}
                rules:
                - topicNames:
                  - one
                  valueRule:
                    syntacticallyCorrectJson: {}
                    jwsSignatureValidationConfig:
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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, new JwsSignatureValidationConfig(ECDSA_JWKS, null, null, null, false), true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, true, false), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, null, true, false)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeNonDefaultValuesJwsSignatureValidation() throws JsonProcessingException {
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
                            jwsSignatureValidationConfig:
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
                                jwsHeaderName: x-jws
                                isContentDetached: true
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
                        new JwsSignatureValidationConfig(RSA_AND_ECDSA_JWKS, AlgorithmConstraints.ConstraintType.PERMIT,
                                new String[]{ AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, AlgorithmIdentifiers.RSA_USING_SHA256 }, "x-jws", true),
                        false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
        assertEquals(expected, deserialised);
    }
}
