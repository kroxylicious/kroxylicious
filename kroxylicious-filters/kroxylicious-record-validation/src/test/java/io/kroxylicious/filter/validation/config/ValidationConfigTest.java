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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidationConfigTest {

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

}
