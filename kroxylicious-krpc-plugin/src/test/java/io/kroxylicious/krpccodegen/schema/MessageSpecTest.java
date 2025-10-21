/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class MessageSpecTest {

    private MessageSpec groupIdMessageSpec;
    private MessageSpec nestedGroupIdMessageSpec;
    private MessageSpec groupIdArrayMessageSpec;
    private MessageSpec topicNameMessageSpec;
    private MessageSpec nestedTopicNameMessageSpec;

    @BeforeEach
    void setUp() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        groupIdMessageSpec = mapper.readValue("""
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
                """, MessageSpec.class);

        groupIdArrayMessageSpec = mapper.readValue("""
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
                """, MessageSpec.class);

        nestedGroupIdMessageSpec = mapper.readValue("""
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
                """, MessageSpec.class);
        topicNameMessageSpec = mapper.readValue("""
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
                """, MessageSpec.class);
        nestedTopicNameMessageSpec = mapper.readValue("""
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
                """, MessageSpec.class);
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
        var versions = groupIdMessageSpec.intersectedVersionsForEntityFields(Set.of(EntityType.GROUP_ID));

        // Then
        assertThat(versions).containsExactly((short) 1, (short) 2, (short) 3, (short) 4);
    }

    @Test
    void shouldReportEmptyIntersectedVersionForMessageWithoutEntityTypeField() {
        // Given

        // When
        var versions = groupIdMessageSpec.intersectedVersionsForEntityFields(Set.of(EntityType.UNKNOWN));

        // Then
        assertThat(versions).isEmpty();
    }

    @Test
    void shouldReportIntersectedVersionForNestedEntityField() {
        // Given

        // When
        var versions = nestedGroupIdMessageSpec.intersectedVersionsForEntityFields(Set.of(EntityType.GROUP_ID));

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
}