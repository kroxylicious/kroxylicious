/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class MessageSpecTest {

    private MessageSpec groupIdMessageSpec;
    private MessageSpec nestedGroupIdMessageSpec;
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
                    "validVersions": "0-9",
                    "flexibleVersions": "6+",
                    "fields": [
                      { "name": "GroupId", "type": "string", "versions": "0+", "entityType": "groupId",
                        "about": "The group identifier." }
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
    void shouldExtractGroupID() {
        // Given

        // When
        Node actualNode = groupIdMessageSpec.entityFields();

        // Then
        assertThat(actualNode.hasAtLeastOneEntityField()).isTrue();
        assertThat(actualNode.entities())
                .singleElement(InstanceOfAssertFactories.type(FieldSpec.class))
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.GROUP_ID);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.StringFieldType.class);
                });
    }

    @Test
    @Disabled("need to decide how we are dealing with nesting")
    void shouldExtractNestedGroupID() {
        // Given

        // When
        Node actualNode = nestedGroupIdMessageSpec.entityFields();

        // Then
        assertThat(actualNode.hasAtLeastOneEntityField()).isTrue();
        assertThat(actualNode.entities())
                .singleElement(InstanceOfAssertFactories.type(FieldSpec.class))
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.GROUP_ID);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.StringFieldType.class);
                });
    }

    @Test
    void shouldExtractTopicName() {
        // Given

        // When
        Node actualNode = topicNameMessageSpec.entityFields();

        // Then
        assertThat(actualNode.hasAtLeastOneEntityField()).isTrue();
        assertThat(actualNode.entities())
                .singleElement(InstanceOfAssertFactories.type(FieldSpec.class))
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.TOPIC_NAME);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.StringFieldType.class);
                });
    }

    @Test
    @Disabled("need to decide how we are dealing with nesting")
    void shouldExtractNestedTopicName() {
        // Given

        // When
        Node actualNode = nestedTopicNameMessageSpec.entityFields();

        // Then
        assertThat(actualNode.hasAtLeastOneEntityField()).isTrue();
        assertThat(actualNode.entities())
                .singleElement(InstanceOfAssertFactories.type(FieldSpec.class))
                .satisfies(fieldSpec -> {
                    assertThat(fieldSpec.entityType()).isEqualTo(EntityType.TOPIC_NAME);
                    assertThat(fieldSpec.type()).isInstanceOf(FieldType.StringFieldType.class);
                });
    }
}