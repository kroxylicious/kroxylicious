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

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.filter.validation.validators.bytebuf.JwsSignatureBytebufValidator;
import io.kroxylicious.proxy.config.tls.AllowDeny;

import static io.kroxylicious.test.jws.JwsTestUtils.ECDSA_VERIFY_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.RSA_AND_ECDSA_VERIFY_JWKS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidationConfigTest {

    private static ValidationConfig expectedApicurioConfig() throws MalformedURLException {
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), new SchemaValidationConfig(URI.create("http://localhost:8080").toURL(), 1L),
                        new JwsSignatureValidationConfig(ECDSA_VERIFY_JWKS, null, null, null), false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        return new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
    }

    private static ValidationConfig expectedJwsSignatureConfig() throws MalformedURLException {
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, new JwsSignatureValidationConfig(RSA_AND_ECDSA_VERIFY_JWKS,
                        new AllowDeny<>(List.of(AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, AlgorithmIdentifiers.RSA_USING_SHA256),
                                null),
                        new JwsSignatureBytebufValidator.JwsHeaderOptions("kroxylicious.io/jws", true),
                        new JwsSignatureBytebufValidator.JwsContentOptions(false)), false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true), null);
        return new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
    }

    public static Stream<Arguments> deserialize() throws MalformedURLException {
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, new JwsSignatureValidationConfig(ECDSA_VERIFY_JWKS, null, null, null), true,
                        false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, true, false), null);
        ValidationConfig defaultConfig = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, null, true, false)));

        TopicMatchingRecordValidationRule nonDefaultRuleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, new JwsSignatureValidationConfig(ECDSA_VERIFY_JWKS, null, null, null), false,
                        true));
        TopicMatchingRecordValidationRule nonDefaultRuleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, null, false, true),
                null);
        ValidationConfig nonDefaultConfig = new ValidationConfig(List.of(nonDefaultRuleOne, nonDefaultRuleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, null, false, true)));
        return Stream.of(Arguments.argumentSet("default values", """
                defaultRule:
                  valueRule: {}
                rules:
                - topicNames:
                  - one
                  valueRule:
                    syntacticallyCorrectJson: {}
                    jwsSignatureValidation:
                        trustedJsonWebKeySet: >
                            %s
                - topicNames:
                  - two
                  keyRule: {}
                """.formatted(ECDSA_VERIFY_JWKS.toJson()), defaultConfig),
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
                            jwsSignatureValidation:
                                trustedJsonWebKeySet: >
                                    %s
                            allowNulls: false
                            allowEmpty: true
                        - topicNames:
                          - two
                          keyRule:
                            allowNulls: false
                            allowEmpty: true
                        """.formatted(ECDSA_VERIFY_JWKS.toJson()), nonDefaultConfig),
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
                            jwsSignatureValidation:
                                trustedJsonWebKeySet: >
                                    %s
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
                        """.formatted(ECDSA_VERIFY_JWKS.toJson()), expectedApicurioConfig()),
                Arguments.argumentSet("non default jws signature values", """
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
                            jwsSignatureValidation:
                                trustedJsonWebKeySet: >
                                    %s
                                algorithms:
                                    allowed:
                                        - ES256
                                        - RS256
                                recordHeader:
                                  key: kroxylicious.io/jws
                                  required: true
                                content:
                                  detached: false
                            allowNulls: false
                            allowEmpty: true
                        - topicNames:
                          - two
                          keyRule:
                            allowNulls: false
                            allowEmpty: true
                        """.formatted(RSA_AND_ECDSA_VERIFY_JWKS.toJson()), expectedJwsSignatureConfig()));
    }

    @MethodSource
    @ParameterizedTest
    void deserialize(String content, ValidationConfig expected) throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue(content);
        assertEquals(expected, deserialised);
    }

}
