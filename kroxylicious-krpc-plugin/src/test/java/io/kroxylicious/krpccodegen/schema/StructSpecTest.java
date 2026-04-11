/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.io.UncheckedIOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StructSpecTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static StructSpec parseStructSpec(String json) {
        try {
            return MAPPER.readValue(json, StructSpec.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void shouldParseValidStructSpec() {
        // when
        var spec = parseStructSpec("""
                { "name": "TestStruct", "versions": "0+", "fields": [
                    { "name": "Field1", "type": "string", "versions": "0+" }
                ]}
                """);

        // then
        assertThat(spec.name()).isEqualTo("TestStruct");
        assertThat(spec.fields()).singleElement()
                .extracting(FieldSpec::name)
                .isEqualTo("Field1");
    }

    @Test
    void shouldRejectMissingVersions() {
        // when/then
        assertThatThrownBy(() -> parseStructSpec("""
                { "name": "TestStruct", "fields": [] }
                """))
                .rootCause()
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("TestStruct");
    }

    @Test
    void shouldRejectDuplicateTagIds() {
        // when/then
        assertThatThrownBy(() -> parseStructSpec("""
                { "name": "TestStruct", "versions": "0+", "fields": [
                    { "name": "Field1", "type": "string", "versions": "0+", "tag": 0, "taggedVersions": "0+" },
                    { "name": "Field2", "type": "string", "versions": "0+", "tag": 0, "taggedVersions": "0+" }
                ]}
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate tag ID");
    }

    @Test
    void shouldRejectDuplicateFieldNames() {
        // when/then
        assertThatThrownBy(() -> parseStructSpec("""
                { "name": "TestStruct", "versions": "0+", "fields": [
                    { "name": "Field1", "type": "string", "versions": "0+" },
                    { "name": "Field1", "type": "int32", "versions": "0+" }
                ]}
                """))
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate name");
    }

    @Test
    void shouldRejectNonContiguousTagIds() {
        // when/then
        assertThatThrownBy(() -> parseStructSpec("""
                { "name": "TestStruct", "versions": "0+", "fields": [
                    { "name": "Field1", "type": "string", "versions": "0+", "tag": 0, "taggedVersions": "0+" },
                    { "name": "Field2", "type": "string", "versions": "0+", "tag": 2, "taggedVersions": "0+" }
                ]}
                """))
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not contiguous");
    }
}
