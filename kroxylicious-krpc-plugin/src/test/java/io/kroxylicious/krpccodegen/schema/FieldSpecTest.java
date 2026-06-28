/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.io.UncheckedIOException;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FieldSpecTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static FieldSpec definitionToFieldSpec(String content) {
        try {
            return MAPPER.readValue(content, FieldSpec.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void shouldExtractField() {
        var fieldSpec = definitionToFieldSpec("""
                        { "name": "Name", "type": "string", "versions": "0+", "mapKey": true,
                          "about": "The configuration key name." }
                """);

        assertThat(fieldSpec.name()).isEqualTo("Name");
    }

    @Test
    void shouldExtractSubFields() {
        var fieldSpec = definitionToFieldSpec("""
                      { "name": "Configs", "type": "[]AlterableConfig", "versions": "0+",
                        "about": "The configurations.",  "fields": [
                        { "name": "Name", "type": "string", "versions": "0+", "mapKey": true,
                          "about": "The configuration key name." },
                        { "name": "ConfigOperation", "type": "int8", "versions": "0+", "mapKey": true,
                          "about": "The type (Set, Delete, Append, Subtract) of operation." },
                        { "name": "Value", "type": "string", "versions": "0+", "nullableVersions": "0+",
                          "about": "The value to set for the configuration key."}
                      ]}
                """);

        assertThat(fieldSpec.fields())
                .extracting(FieldSpec::name)
                .containsExactly("Name", "ConfigOperation", "Value");
    }

    @Test
    void shouldDetectEntityField() {
        var fieldSpec = definitionToFieldSpec("""
                  { "name": "GroupId", "type": "string", "versions": "1+", "entityType": "groupId",
                    "about": "The group identifier." }
                """);

        assertThat(fieldSpec)
                .satisfies(fs -> {
                    assertThat(fs.name()).isEqualTo("GroupId");
                    assertThat(fs.entityType()).isEqualTo(EntityType.GROUP_ID);
                    assertThat(fs.hasAtLeastOneEntityField(Set.of(EntityType.GROUP_ID))).isTrue();
                });
    }

    @Test
    void shouldDetectNestedEntityField() {
        var fieldSpec = definitionToFieldSpec("""
                     { "name": "Groups", "type": "[]DescribedGroup", "versions": "0+",
                       "about": "Each described group.",
                       "fields": [
                         { "name": "GroupId", "type": "string", "versions": "0+", "entityType": "groupId",
                           "about": "The group ID string." }
                       ]}
                """);

        assertThat(fieldSpec)
                .satisfies(fs -> {
                    assertThat(fs.name()).isEqualTo("Groups");
                    assertThat(fs.entityType()).isEqualTo(EntityType.UNKNOWN);
                    assertThat(fs.hasAtLeastOneEntityField(Set.of(EntityType.GROUP_ID))).isTrue();
                    assertThat(fs.fields())
                            .singleElement()
                            .satisfies(sf -> {
                                assertThat(sf.name()).isEqualTo("GroupId");
                                assertThat(sf.entityType()).isEqualTo(EntityType.GROUP_ID);
                                assertThat(sf.hasAtLeastOneEntityField(Set.of(EntityType.GROUP_ID))).isTrue();
                            });
                });
    }

    static Stream<Arguments> resourceListScenarios() {
        return Stream.of(Arguments.argumentSet("example response with resource list", definitionToFieldSpec("""
                    { "name": "Resources", "type": "[]AlterConfigsResource", "versions": "0+",
                      "about": "The incremental updates for each resource.", "fields": [
                      { "name": "ResourceType", "type": "int8", "versions": "0+", "mapKey": true,
                        "about": "The resource type." },
                      { "name": "ResourceName", "type": "string", "versions": "0+", "mapKey": true,
                        "about": "The resource name." },
                      { "name": "Configs", "type": "[]AlterableConfig", "versions": "0+",
                        "about": "The configurations.",  "fields": [
                        { "name": "Name", "type": "string", "versions": "0+", "mapKey": true,
                          "about": "The configuration key name." },
                        { "name": "ConfigOperation", "type": "int8", "versions": "0+", "mapKey": true,
                          "about": "The type (Set, Delete, Append, Subtract) of operation." },
                        { "name": "Value", "type": "string", "versions": "0+", "nullableVersions": "0+",
                          "about": "The value to set for the configuration key."}
                      ]}
                    ]}
                """), true),
                Arguments.argumentSet("example request with resource list", definitionToFieldSpec("""
                            { "name": "Creations", "type": "[]AclCreation", "versions": "0+",
                              "about": "The ACLs that we want to create.", "fields": [
                              { "name": "ResourceType", "type": "int8", "versions": "0+",
                                "about": "The type of the resource." },
                              { "name": "ResourceName", "type": "string", "versions": "0+",
                                "about": "The resource name for the ACL." },
                              { "name": "ResourcePatternType", "type": "int8", "versions": "1+", "default": "3",
                                "about": "The pattern type for the ACL." },
                              { "name": "Principal", "type": "string", "versions": "0+",
                                "about": "The principal for the ACL." },
                              { "name": "Host", "type": "string", "versions": "0+",
                                "about": "The host for the ACL." },
                              { "name": "Operation", "type": "int8", "versions": "0+",
                                "about": "The operation type for the ACL (read, write, etc.)." },
                              { "name": "PermissionType", "type": "int8", "versions": "0+",
                                "about": "The permission type for the ACL (allow, deny, etc.)." }
                            ]}
                        """), true),
                Arguments.argumentSet("unexpected resourceName type", definitionToFieldSpec("""
                            { "name": "Resources", "type": "[]AlterConfigsResource", "versions": "0+",
                              "about": "The incremental updates for each resource.", "fields": [
                              { "name": "ResourceType", "type": "int8", "versions": "0+", "mapKey": true,
                                "about": "The resource type." },
                              { "name": "ResourceName", "type": "int8", "versions": "0+", "mapKey": true,
                                "about": "The resource name." }
                          ]}}
                        """), false),
                Arguments.argumentSet("missing resourceName field", definitionToFieldSpec("""
                            { "name": "Resources", "type": "[]AlterConfigsResource", "versions": "0+",
                              "about": "The incremental updates for each resource.", "fields": [
                              { "name": "ResourceType", "type": "int8", "versions": "0+", "mapKey": true,
                                "about": "The resource type." }
                          ]}}
                        """), false),
                Arguments.argumentSet("example request with non-standard resource list", definitionToFieldSpec("""
                            { "name": "Filters", "type": "[]DeleteAclsFilter", "versions": "0+",
                              "about": "The filters to use when deleting ACLs.", "fields": [
                              { "name": "ResourceTypeFilter", "type": "int8", "versions": "0+",
                                "about": "The resource type." },
                              { "name": "ResourceNameFilter", "type": "string", "versions": "0+", "nullableVersions": "0+",
                                "about": "The resource name." },
                              { "name": "PatternTypeFilter", "type": "int8", "versions": "1+", "default": "3", "ignorable": false,
                                "about": "The pattern type." },
                              { "name": "PrincipalFilter", "type": "string", "versions": "0+", "nullableVersions": "0+",
                                "about": "The principal filter, or null to accept all principals." },
                              { "name": "HostFilter", "type": "string", "versions": "0+", "nullableVersions": "0+",
                                "about": "The host filter, or null to accept all hosts." },
                              { "name": "Operation", "type": "int8", "versions": "0+",
                                "about": "The ACL operation." },
                              { "name": "PermissionType", "type": "int8", "versions": "0+",
                                "about": "The permission type." }
                            ]}
                        """), false));
    }

    @ParameterizedTest
    @MethodSource("resourceListScenarios")
    void shouldDetectResourceListInFieldHierarchy(FieldSpec fs, boolean hasResourceList) {
        // When/Then
        assertThat(fs.isResourceList()).isEqualTo(hasResourceList);
    }

    @Test
    void shouldRejectInvalidFieldName() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "123invalid", "type": "string", "versions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid field name");
    }

    @Test
    void shouldRequireVersions() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("You must specify the version");
    }

    @Test
    void shouldRejectFlexibleVersionsOnNonStringOrBytesField() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "int32", "versions": "0+", "flexibleVersions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid flexibleVersions override");
    }

    @Test
    void shouldRejectNullableVersionsOnNonNullableType() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "int32", "versions": "0+", "nullableVersions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be nullable");
    }

    @Test
    void shouldRejectSubFieldsOnNonArrayOrStructField() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "fields": [
                    { "name": "Sub", "type": "string", "versions": "0+" }
                ]}
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot have fields");
    }

    @Test
    void shouldRejectTaggedFieldUsedAsMapKey() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "mapKey": true, "tag": 0, "taggedVersions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Tagged fields cannot be used as keys");
    }

    @Test
    void shouldRejectZeroCopyOnNonBytesField() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "zeroCopy": true }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only fields of type bytes can use zeroCopy");
    }

    @Test
    void shouldRejectNegativeTag() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "tag": -1, "taggedVersions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Tags cannot be negative");
    }

    @Test
    void shouldRequireTaggedVersionsWhenTagIsSpecified() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "tag": 0 }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taggedVersions must be specified");
    }

    @Test
    void shouldRejectNonOpenEndedTaggedVersions() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "tag": 0, "taggedVersions": "0-5" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taggedVersions must be either none, or an open-ended range");
    }

    @Test
    void shouldRejectTaggedVersionsOutsideVersions() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0-3", "tag": 0, "taggedVersions": "4+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taggedVersions must be a subset of versions");
    }

    @Test
    void shouldRejectPartialNullableTaggedVersionsOverlap() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "nullableVersions": "0-5", "tag": 0, "taggedVersions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either all tagged versions must be nullable, or none must be");
    }

    @Test
    void shouldRejectTaggedVersionsWithoutTag() {
        assertThatThrownBy(() -> definitionToFieldSpec("""
                { "name": "Name", "type": "string", "versions": "0+", "taggedVersions": "0+" }
                """))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not specify a tag");
    }

}