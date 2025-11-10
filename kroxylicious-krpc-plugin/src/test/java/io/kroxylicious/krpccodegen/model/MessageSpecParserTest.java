/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.FieldSpec;
import io.kroxylicious.krpccodegen.schema.FieldType;
import io.kroxylicious.krpccodegen.schema.MessageSpecType;
import io.kroxylicious.krpccodegen.schema.RequestListenerType;
import io.kroxylicious.krpccodegen.schema.Versions;

import static org.assertj.core.api.Assertions.assertThat;

class MessageSpecParserTest {

    private final MessageSpecParser messageSpecParser = new MessageSpecParser();

    @Test
    void parseSpecWithOneField() throws Exception {
        var resource = classPathResourceToPath("io/kroxylicious/krpccondegen/model/RequestWithTopLevelField.json");
        var messageSpec = messageSpecParser.getMessageSpec(resource);
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.name()).isEqualTo("SampleRequest");
                    assertThat(ms.listeners()).containsExactlyInAnyOrder(RequestListenerType.BROKER, RequestListenerType.CONTROLLER);
                    assertThat(ms.validVersions()).isEqualTo(Versions.parse("0-1", null));
                    assertThat(ms.apiKey()).isPresent()
                            .get(InstanceOfAssertFactories.type(Short.class))
                            .isEqualTo((short) 1);
                    assertThat(ms.fields())
                            .singleElement()
                            .satisfies(field -> {
                                assertThat(field.name()).isEqualTo("MyField");
                            });
                });
    }

    @Test
    void parseResponseSpecWithOneField() throws Exception {
        // Given
        Versions expectedVersion = Versions.parse("0-9", null);

        // When
        var messageSpec = messageSpecParser.getMessageSpec(classPathResourceToInputStream("common/message/JoinGroupResponse.json"));

        // Then
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.name()).isEqualTo("JoinGroupResponse");
                    assertThat(ms.type()).isEqualTo(MessageSpecType.RESPONSE);
                    assertThat(ms.validVersions()).isEqualTo(expectedVersion);
                    assertThat(ms.apiKey())
                            .isPresent()
                            .get(InstanceOfAssertFactories.type(Short.class))
                            .isEqualTo(ApiKeys.JOIN_GROUP.id);
                    assertThat(ms.fields())
                            .hasSize(9);
                    assertThat(ms.entityFields())
                            .satisfies(node -> {
                                assertThat(node.hasAtLeastOneEntityField()).isFalse(); // TODO should this be false?
                            });
                    Map<String, FieldSpec> msFieldsByName = ms.fieldsByName();
                    assertThat(msFieldsByName)
                            .hasEntrySatisfying("ThrottleTimeMs",
                                    field -> assertExpectedFieldSpec(field, "ThrottleTimeMs", FieldType.Int32FieldType.class, EntityType.UNKNOWN))
                            .hasEntrySatisfying("Members", field -> {
                                assertExpectedFieldSpec(field, "Members", FieldType.ArrayType.class, EntityType.UNKNOWN);
                                assertThat(field.fields()).hasSize(3);
                                var fsFieldsByName = field.fieldsByName();
                                assertThat(fsFieldsByName)
                                        .hasSize(3)
                                        .hasEntrySatisfying("MemberId",
                                                subField -> assertExpectedFieldSpec(subField, "MemberId", FieldType.StringFieldType.class, EntityType.UNKNOWN))
                                        .hasEntrySatisfying("GroupInstanceId",
                                                subField -> assertExpectedFieldSpec(subField, "GroupInstanceId", FieldType.StringFieldType.class, EntityType.UNKNOWN))
                                        .hasEntrySatisfying("Metadata",
                                                subField -> assertExpectedFieldSpec(subField, "Metadata", FieldType.BytesFieldType.class, EntityType.UNKNOWN));
                            });
                });
    }

    private static void assertExpectedFieldSpec(FieldSpec field, String expectedFieldName, Class<? extends FieldType> expectedFieldType, EntityType expectedEntityType) {
        assertThat(field.name()).isEqualTo(expectedFieldName);
        assertThat(field.type()).isInstanceOf(expectedFieldType);
        assertThat(field.entityType()).isEqualTo(expectedEntityType);
    }

    @Test
    void parseListField() throws Exception {
        var resource = classPathResourceToPath("io/kroxylicious/krpccondegen/model/RequestWithListWithOneField.json");
        var messageSpec = messageSpecParser.getMessageSpec(resource);
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.name()).isEqualTo("ListFieldRequest");
                    assertThat(ms.fields())
                            .singleElement()
                            .satisfies(field -> {
                                assertThat(field.name()).isEqualTo("Topics");
                                var subFields = field.fields();
                                assertThat(subFields)
                                        .singleElement()
                                        .satisfies(sf -> {
                                            assertThat(sf.name()).isEqualTo("Name");
                                            assertThat(sf.entityType()).isEqualTo(EntityType.TOPIC_NAME);
                                        });
                            });
                });
    }

    @Test
    void parseSpecWithTopLevelEntityField() throws Exception {
        var resource = classPathResourceToPath("io/kroxylicious/krpccondegen/model/RequestWithTopLevelEntityField.json");
        var messageSpec = messageSpecParser.getMessageSpec(resource);
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.name()).isEqualTo("SampleRequest");
                    assertThat(ms.fields())
                            .hasSize(2);

                    assertThat(ms.entityFields())
                            .satisfies(node -> {
                                assertThat(node.hasAtLeastOneEntityField()).isTrue();
                                assertThat(node.orderedVersions()).containsExactly((short) 0, (short) 1);
                                assertThat(node.entities())
                                        .singleElement()
                                        .satisfies(field -> {
                                            assertThat(field.name()).isEqualTo("GroupId");
                                        });
                                assertThat(node.containers()).isEmpty();
                            });
                });
    }

    @Test
    void parseSpecWithTopLevelEntityFieldArray() throws Exception {
        var resource = classPathResourceToPath("io/kroxylicious/krpccondegen/model/RequestWithEntityFieldArray.json");
        var messageSpec = messageSpecParser.getMessageSpec(resource);
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.name()).isEqualTo("SampleRequest");
                    assertThat(ms.fields())
                            .hasSize(1);

                    assertThat(ms.entityFields())
                            .satisfies(node -> {
                                assertThat(node.hasAtLeastOneEntityField()).isTrue();
                                assertThat(node.orderedVersions()).containsExactly((short) 0, (short) 1);
                                assertThat(node.entities())
                                        .singleElement()
                                        .satisfies(field -> {
                                            assertThat(field.name()).isEqualTo("Groups");
                                        });
                                assertThat(node.containers()).isEmpty();
                            });
                });
    }

    @Test
    void parseSpecWithSeveralTopLevelEntityFieldsWithDistinctVersionRanges() throws Exception {
        var resource = classPathResourceToPath("io/kroxylicious/krpccondegen/model/RequestWithSeveralTopLevelEntityFieldsWithDistinctVersionRanges.json");
        var messageSpec = messageSpecParser.getMessageSpec(resource);
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.entityFields())
                            .satisfies(node -> {
                                assertThat(node.hasAtLeastOneEntityField()).isTrue();
                                assertThat(node.orderedVersions()).containsExactly((short) 0, (short) 1, (short) 3, (short) 4);
                            });
                });
    }

    private Path classPathResourceToPath(String resource) {
        var url = getClass().getClassLoader().getResource(resource);
        assertThat(url).isNotNull();
        return new File(url.getFile()).toPath();
    }

    private InputStream classPathResourceToInputStream(String resource) {
        return getClass().getClassLoader().getResourceAsStream(resource);
    }

}