/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.io.File;
import java.nio.file.Path;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.kroxylicious.krpccodegen.schema.EntityType;
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
                    assertThat(ms.apiKey()).
                            isPresent()
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
                                assertThat(node.orderedVersions()).containsExactly((short)  0, (short) 1);
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
    void parseSpecWithSeveralTopLevelEntityFieldsWithDistinctVersionRanges() throws Exception {
        var resource = classPathResourceToPath("io/kroxylicious/krpccondegen/model/RequestWithSeveralTopLevelEntityFieldsWithDistinctVersionRanges.json");
        var messageSpec = messageSpecParser.getMessageSpec(resource);
        assertThat(messageSpec)
                .isNotNull()
                .satisfies(ms -> {
                    assertThat(ms.entityFields())
                            .satisfies(node -> {
                                assertThat(node.hasAtLeastOneEntityField()).isTrue();
                                assertThat(node.orderedVersions()).containsExactly((short)  0, (short) 1, (short) 3, (short) 4);
                            });
                });
    }


    private Path classPathResourceToPath(String resource) {
        var url = getClass().getClassLoader().getResource(resource);
        assertThat(url).isNotNull();
        return new File(url.getFile()).toPath();
    }

}