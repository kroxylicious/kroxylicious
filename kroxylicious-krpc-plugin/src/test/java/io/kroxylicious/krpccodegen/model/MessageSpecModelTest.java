/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;

import freemarker.ext.beans.GenericObjectModel;
import freemarker.template.SimpleScalar;
import freemarker.template.SimpleSequence;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageSpecModelTest {

    private static final KrpcSchemaObjectWrapper WRAPPER = new KrpcSchemaObjectWrapper(new Version(2, 3, 34));

    @Mock
    private MessageSpec structSpec;

    @Test
    void name() throws Exception {
        // Given
        when(structSpec.name()).thenReturn("foo");
        var model = new MessageSpecModel(WRAPPER, structSpec);

        // WHen
        var result = model.get("name");
        assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.type(SimpleScalar.class))
                .extracting(SimpleScalar::getAsString)
                .isEqualTo("foo");
    }

    static Stream<Arguments> hasAtLeastOneEntityField() {
        return Stream.of(
                Arguments.argumentSet("called with simple sequence", List.of(new SimpleSequence(List.of(new SimpleScalar("TOPIC_NAME")), WRAPPER))),
                Arguments.argumentSet("called with singleton", List.of(new GenericObjectModel("TOPIC_NAME", WRAPPER))));
    }

    @ParameterizedTest
    @MethodSource(value = "hasAtLeastOneEntityField")
    void hasAtLeastOneEntityField(List methodArgs) throws Exception {
        // Given
        when(structSpec.hasAtLeastOneEntityField(Set.of(EntityType.TOPIC_NAME))).thenReturn(true);

        var model = new MessageSpecModel(WRAPPER, structSpec);
        var templateMethod = (TemplateMethodModelEx) model.get("hasAtLeastOneEntityField");

        // When
        var result = templateMethod.exec(methodArgs);

        // Then
        assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.type(Boolean.class))
                .isEqualTo(true);

        verify(structSpec).hasAtLeastOneEntityField(Set.of(EntityType.TOPIC_NAME));
    }

    @Test
    void hasAtLeastOneEntityFieldDetectsUnknownEntityType() throws Exception {
        // Given
        var model = new MessageSpecModel(WRAPPER, structSpec);
        var templateMethod = (TemplateMethodModelEx) model.get("hasAtLeastOneEntityField");
        var entityTypeSeq = new SimpleSequence(List.of(new SimpleScalar("NotAKnownType")), WRAPPER);
        var methodArgs = List.of(entityTypeSeq);

        // When/Then
        assertThatThrownBy(() -> templateMethod.exec(methodArgs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No enum constant io.kroxylicious.krpccodegen.schema.EntityType.NotAKnownType");

        verify(structSpec, never()).hasAtLeastOneEntityField(anySet());
    }

    @Test
    void intersectedVersionsForEntityFields() throws Exception {
        // Given
        when(structSpec.intersectedVersionsForEntityFields(Set.of(EntityType.TOPIC_NAME))).thenReturn(List.of((short) 1));

        var model = new MessageSpecModel(WRAPPER, structSpec);
        var templateMethod = (TemplateMethodModelEx) model.get("intersectedVersionsForEntityFields");
        var entityTypeSeq = new SimpleSequence(List.of(new SimpleScalar("TOPIC_NAME")), WRAPPER);
        var methodArgs = List.of(entityTypeSeq);

        // When
        var result = templateMethod.exec(methodArgs);

        // Then
        assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.list(Short.class))
                .containsExactly((short) 1);

        verify(structSpec).intersectedVersionsForEntityFields(Set.of(EntityType.TOPIC_NAME));
    }

}