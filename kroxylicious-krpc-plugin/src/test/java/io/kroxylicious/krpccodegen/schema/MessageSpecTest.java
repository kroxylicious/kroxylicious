/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.io.UncheckedIOException;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class MessageSpecTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private MessageSpec groupIdMessageSpec;
    private MessageSpec nestedGroupIdMessageSpec;
    private MessageSpec groupIdArrayMessageSpec;
    private MessageSpec topicNameMessageSpec;
    private MessageSpec nestedTopicNameMessageSpec;

    @BeforeEach
    void setUp() throws JsonProcessingException {
        groupIdMessageSpec = definitionToMessageSpec("""
                  {
                    "apiKey": 11,
                    "type": "request",
                    "listeners": ["broker"],
                    "name": "JoinGroupRequest",
                    "validVersions": "0-4",
                    "flexibleVersions": "6+",
                    "fields": [
                      { "name": "GroupId", "type": "string", "versions": "1+", "entityType": "groupId",
                        "about": "The group identifier." }
                    ]
                }
                """);

        groupIdArrayMessageSpec = definitionToMessageSpec("""
                {
                  "apiKey": 15,
                  "type": "request",
                  "listeners": ["broker"],
                  "name": "SampleRequest",
                  "validVersions": "0-1",
                  "flexibleVersions": "0+",
                  "fields": [
                    { "name": "Groups", "type": "[]string", "versions": "0+", "entityType": "groupId",
                      "about": "The names of the groups to describe." }
                  ]
                }
                """);

        nestedGroupIdMessageSpec = definitionToMessageSpec("""
                {
                   "apiKey": 69,
                   "type": "response",
                   "name": "ConsumerGroupDescribeResponse",
                   "validVersions": "0-1",
                   "flexibleVersions": "0+",
                   "fields": [
                     { "name": "Groups", "type": "[]DescribedGroup", "versions": "0+",
                       "about": "Each described group.",
                       "fields": [
                         { "name": "GroupId", "type": "string", "versions": "0+", "entityType": "groupId",
                           "about": "The group ID string." }
                       ]}
                   ]
                }
                """);
        topicNameMessageSpec = definitionToMessageSpec("""
                  {
                      "apiKey": 47,
                      "type": "request",
                      "listeners": ["broker"],
                      "name": "OffsetDeleteRequest",
                      "validVersions": "0",
                      "flexibleVersions": "none",
                      "fields": [
                            { "name": "Name",  "type": "string",  "versions": "0+", "mapKey": true, "entityType": "topicName",
                              "about": "The topic name." }
                     ]
                }
                """);
        nestedTopicNameMessageSpec = definitionToMessageSpec("""
                  {
                      "apiKey": 47,
                      "type": "request",
                      "listeners": ["broker"],
                      "name": "OffsetDeleteRequest",
                      "validVersions": "0",
                      "flexibleVersions": "none",
                      "fields": [
                        { "name": "Topics", "type": "[]OffsetDeleteRequestTopic", "versions": "0+",
                          "about": "The topics to delete offsets for.", "fields": [
                            { "name": "Name",  "type": "string",  "versions": "0+", "mapKey": true, "entityType": "topicName",
                              "about": "The topic name." }
                              ]
                     }]
                }
                """);
    }

    private static MessageSpec definitionToMessageSpec(String content) {
        try {
            return MAPPER.readValue(content, MessageSpec.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void shouldDetectTopLevelEntityField() {
        // Given

        // When
        var atLeastOneEntityField = groupIdMessageSpec.hasAtLeastOneEntityField(Set.of(EntityType.GROUP_ID));

        // Then
        assertThat(atLeastOneEntityField).isTrue();
    }

    @Test
    void shouldDetectAbsenceOfEntityField() {
        // Given

        // When
        var noEntityFieldOfType = groupIdMessageSpec.hasAtLeastOneEntityField(Set.of(EntityType.TOPIC_NAME));

        // Then
        assertThat(noEntityFieldOfType).isFalse();
    }

    @Test
    void shouldDetectNestedEntityField() {
        // Given

        // When
        var atLeastOneEntityField = nestedGroupIdMessageSpec.hasAtLeastOneEntityField(Set.of(EntityType.GROUP_ID));

        // Then
        assertThat(atLeastOneEntityField).isTrue();
    }

    @Test
    void shouldReportIntersectedVersionForTopLevelEntityField() {
        // Given

        // When
        var versions = groupIdMessageSpec.intersectedVersions(f -> EntityType.GROUP_ID.equals(f.entityType()));

        // Then
        assertThat(versions).containsExactly((short) 1, (short) 2, (short) 3, (short) 4);
    }

    @Test
    void shouldReportEmptyIntersectedVersionForMessageWithoutMatchingField() {
        // Given

        // When
        var versions = groupIdMessageSpec.intersectedVersions(f -> false);

        // Then
        assertThat(versions).isEmpty();
    }

    @Test
    void shouldReportIntersectedVersionForNestedEntityField() {
        // Given

        // When
        var versions = nestedGroupIdMessageSpec.intersectedVersions(f -> EntityType.GROUP_ID.equals(f.entityType()));

        // Then
        assertThat(versions).containsExactly((short) 0, (short) 1);
    }

    @Test
    void shouldExtractGroupID() {
        // Given

        // When
        var fields = groupIdMessageSpec.fields();

        // Then
        assertThat(fields)
                .singleElement()
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.name()).isEqualTo("GroupId");
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.GROUP_ID);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.StringFieldType.class);
                });
    }

    @Test
    void shouldExtractGroupIDArray() {
        // Given

        // When
        var fields = groupIdArrayMessageSpec.fields();

        // Then
        assertThat(fields)
                .singleElement()
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.name()).isEqualTo("Groups");
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.GROUP_ID);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.ArrayType.class);
                });
    }

    @Test
    void shouldExtractNestedGroupID() {
        // Given

        // When
        var fields = nestedGroupIdMessageSpec.fields();

        // Then
        assertThat(fields)
                .singleElement()
                .satisfies(topLevelArrayField -> {
                    assertThat(topLevelArrayField.name()).isEqualTo("Groups");
                    assertThat(topLevelArrayField.entityType()).isEqualTo(EntityType.UNKNOWN);
                    assertThat(topLevelArrayField.type()).isInstanceOf(FieldType.ArrayType.class);
                    assertThat(topLevelArrayField.fields())
                            .singleElement()
                            .satisfies(groupIdField -> {
                                assertThat(groupIdField.name()).isEqualTo("GroupId");
                                assertThat(groupIdField.entityType()).isEqualTo(EntityType.GROUP_ID);
                                assertThat(groupIdField.type()).isInstanceOf(FieldType.StringFieldType.class);
                                assertThat(groupIdField.fields()).isEmpty();

                            });
                });
    }

    @Test
    void shouldExtractTopicName() {
        // Given

        // When
        var fields = topicNameMessageSpec.fields();

        // Then
        assertThat(fields)
                .singleElement()
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.name()).isEqualTo("Name");
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.TOPIC_NAME);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.StringFieldType.class);
                });
    }

    @Test
    void shouldExtractNestedTopicName() {
        // Given

        // When
        var fields = nestedTopicNameMessageSpec.fields();

        // Then
        assertThat(fields)
                .singleElement()
                .satisfies(topLevelArrayField -> {
                    assertThat(topLevelArrayField.name()).isEqualTo("Topics");
                    assertThat(topLevelArrayField.entityType()).isEqualTo(EntityType.UNKNOWN);
                    assertThat(topLevelArrayField.type()).isInstanceOf(FieldType.ArrayType.class);
                    assertThat(topLevelArrayField.fields())
                            .singleElement()
                            .satisfies(groupIdField -> {
                                assertThat(groupIdField.name()).isEqualTo("Name");
                                assertThat(groupIdField.entityType()).isEqualTo(EntityType.TOPIC_NAME);
                                assertThat(groupIdField.type()).isInstanceOf(FieldType.StringFieldType.class);
                                assertThat(groupIdField.fields()).isEmpty();

                            });
                });
    }

    static Stream<Arguments> specsThatDefineResourceLists() {
        return Stream.of(Arguments.argumentSet("request with resource list", definitionToMessageSpec("""
                {
                  "apiKey": 44,
                  "type": "request",
                  "listeners": ["broker", "controller"],
                  "name": "IncrementalAlterConfigsRequest",
                  "validVersions": "0-1",
                  "flexibleVersions": "1+",
                  "fields": [
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
                    ]},
                    { "name": "ValidateOnly", "type": "bool", "versions": "0+",
                      "about": "True if we should validate the request, but not change the configurations."}
                  ]
                }
                """)),
                Arguments.argumentSet("response with resource list", definitionToMessageSpec(
                        """
                                {
                                  "apiKey": 44,
                                  "type": "response",
                                  "name": "IncrementalAlterConfigsResponse",
                                  "validVersions": "0-1",
                                  "flexibleVersions": "1+",
                                  "fields": [
                                    { "name": "ThrottleTimeMs", "type": "int32", "versions": "0+",
                                      "about": "Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota." },
                                    { "name": "Responses", "type": "[]AlterConfigsResourceResponse", "versions": "0+",
                                      "about": "The responses for each resource.", "fields": [
                                      { "name": "ErrorCode", "type": "int16", "versions": "0+",
                                        "about": "The resource error code." },
                                      { "name": "ErrorMessage", "type": "string", "nullableVersions": "0+", "versions": "0+",
                                        "about": "The resource error message, or null if there was no error." },
                                      { "name": "ResourceType", "type": "int8", "versions": "0+",
                                        "about": "The resource type." },
                                      { "name": "ResourceName", "type": "string", "versions": "0+",
                                        "about": "The resource name." }
                                    ]}
                                  ]
                                }
                                """)));
    }

    @ParameterizedTest
    @MethodSource("specsThatDefineResourceLists")
    void shouldFindRequestResourceList(MessageSpec ms) {
        // When/Then
        assertThat(ms.hasResourceList()).isTrue();
    }

    static Stream<Arguments> specsThatDontDefineResourceLists() {
        return Stream.of(Arguments.argumentSet("no container", definitionToMessageSpec("""
                {
                  "apiKey": 99,
                  "type": "request",
                  "listeners": ["broker", "controller"],
                  "name": "Test",
                  "validVersions": "0-1",
                  "flexibleVersions": "1+",
                  "fields": [
                    { "name": "ValidateOnly", "type": "bool", "versions": "0+",
                      "about": "True if we should validate the request, but not change the configurations."}
                  ]
                }
                """)),
                Arguments.argumentSet("no ResourceName", definitionToMessageSpec("""
                        {
                          "apiKey": 99,
                          "type": "request",
                          "listeners": ["broker", "controller"],
                          "name": "Test",
                          "validVersions": "0-1",
                          "flexibleVersions": "1+",
                          "fields": [
                            { "name": "Resources", "type": "[]AlterConfigsResource", "versions": "0+",
                              "about": "The incremental updates for each resource.", "fields": [
                              { "name": "ResourceType", "type": "int8", "versions": "0+", "mapKey": true,
                                "about": "The resource type." }
                            ]},
                            { "name": "ValidateOnly", "type": "bool", "versions": "0+",
                              "about": "True if we should validate the request, but not change the configurations."}
                          ]
                        }
                        """)),
                Arguments.argumentSet("wrong type", definitionToMessageSpec("""
                        {
                          "apiKey": 99,
                          "type": "request",
                          "listeners": ["broker", "controller"],
                          "name": "Test",
                          "validVersions": "0-1",
                          "flexibleVersions": "1+",
                          "fields": [
                            { "name": "Resources", "type": "[]AlterConfigsResource", "versions": "0+",
                              "about": "The incremental updates for each resource.", "fields": [
                              { "name": "ResourceName", "type": "string", "versions": "0+", "mapKey": true,
                                "about": "The resource name." },
                              { "name": "ResourceType", "type": "string", "versions": "0+", "mapKey": true,
                                "about": "The resource type." }
                            ]},
                            { "name": "ValidateOnly", "type": "bool", "versions": "0+",
                              "about": "True if we should validate the request, but not change the configurations."}
                          ]
                        }
                        """)),
                Arguments.argumentSet("field version mismatch", definitionToMessageSpec("""
                        {
                          "apiKey": 99,
                          "type": "request",
                          "listeners": ["broker", "controller"],
                          "name": "Test",
                          "validVersions": "0-1",
                          "flexibleVersions": "1+",
                          "fields": [
                            { "name": "Resources", "type": "[]AlterConfigsResource", "versions": "0+",
                              "about": "The incremental updates for each resource.", "fields": [
                              { "name": "ResourceName", "type": "string", "versions": "0+", "mapKey": true,
                                "about": "The resource name." },
                              { "name": "ResourceType", "type": "string", "versions": "1+", "mapKey": true,
                                "about": "The resource type." }
                            ]},
                            { "name": "ValidateOnly", "type": "bool", "versions": "0+",
                              "about": "True if we should validate the request, but not change the configurations."}
                          ]
                        }
                        """)));
    }

    @ParameterizedTest
    @MethodSource("specsThatDontDefineResourceLists")
    void shouldNotFindRequestResourceList(MessageSpec ms) {
        // When/Then
        assertThat(ms.hasResourceList()).isFalse();
    }
}