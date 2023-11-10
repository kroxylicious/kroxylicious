/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.config;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidationConfigTest {

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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, true, false), null);
        ValidationConfig expected = new ValidationConfig(false, List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, true, false)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeNonDefaultValues() throws JsonProcessingException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
                forwardPartialRequests: true
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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), false, true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, false, true), null);
        ValidationConfig expected = new ValidationConfig(true, List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, false, true)));
        assertEquals(expected, deserialised);
    }

}