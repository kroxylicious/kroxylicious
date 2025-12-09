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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;

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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, true, false), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, true, false)));
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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, false, true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, false, true)));
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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), null, true, false));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, true, false), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo),
                new RecordValidationRule(null, new BytebufValidation(null, null, true, false)));
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
                        apicurioContentId: 1
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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), new SchemaValidationConfig(URI.create("http://localhost:8080").toURL(), 1L, null, null),
                        false,
                        true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, false, true)));
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
                new BytebufValidation(new SyntacticallyCorrectJsonConfig(true), null, false, true));
        TopicMatchingRecordValidationRule ruleTwo = new TopicMatchingRecordValidationRule(Set.of("two"), new BytebufValidation(null, null, false, true), null);
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne, ruleTwo), new RecordValidationRule(null, new BytebufValidation(null, null, false, true)));
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeSchemaValidationWithTlsTrustStore() throws JsonProcessingException, MalformedURLException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
                rules:
                - topicNames:
                  - one
                  valueRule:
                    schemaValidationConfig:
                        apicurioContentId: 1
                        apicurioRegistryUrl: https://secure-registry.example.com
                        tls:
                          trust:
                            storeFile: /path/to/truststore.p12
                            storeType: PKCS12
                """);

        Tls expectedTls = new Tls(null, new TrustStore("/path/to/truststore.p12", null, "PKCS12"), null, null);
        SchemaValidationConfig expectedSchemaConfig = new SchemaValidationConfig(
                URI.create("https://secure-registry.example.com").toURL(), 1L, null, expectedTls);
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(null, expectedSchemaConfig, true, false));
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne), null);
        assertEquals(expected, deserialised);
    }

    @Test
    void testDecodeSchemaValidationWithInsecureTls() throws JsonProcessingException, MalformedURLException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ValidationConfig deserialised = yamlMapper.readerFor(ValidationConfig.class).readValue("""
                rules:
                - topicNames:
                  - one
                  valueRule:
                    schemaValidationConfig:
                        apicurioContentId: 1
                        apicurioRegistryUrl: https://secure-registry.example.com
                        tls:
                          trust:
                            insecure: true
                """);

        Tls expectedTls = new Tls(null, new InsecureTls(true), null, null);
        SchemaValidationConfig expectedSchemaConfig = new SchemaValidationConfig(
                URI.create("https://secure-registry.example.com").toURL(), 1L, null, expectedTls);
        TopicMatchingRecordValidationRule ruleOne = new TopicMatchingRecordValidationRule(Set.of("one"), null,
                new BytebufValidation(null, expectedSchemaConfig, true, false));
        ValidationConfig expected = new ValidationConfig(List.of(ruleOne), null);
        assertEquals(expected, deserialised);
    }
}
