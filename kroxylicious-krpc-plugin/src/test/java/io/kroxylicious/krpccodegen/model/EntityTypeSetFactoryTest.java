/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.List;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.kroxylicious.krpccodegen.schema.EntityType;

import freemarker.template.SimpleScalar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityTypeSetFactoryTest {

    @Test
    void generatesEntityTypeSet() {
        // Given
        var model = new EntityTypeSetFactory();
        var methodArgs = Stream.of("TOPIC_NAME", "GROUP_ID").map(SimpleScalar::new).toList();

        // When
        var result = model.exec(methodArgs);

        // Then
        assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.set(EntityType.class))
                .containsExactly(EntityType.TOPIC_NAME, EntityType.GROUP_ID);
    }

    @Test
    void shouldDetectUnrecognizedEntityType() {
        // Given
        var model = new EntityTypeSetFactory();
        var methodArgs = List.of(new SimpleScalar("NotKnown"));

        // When/Then
        assertThatThrownBy(() -> model.exec(methodArgs))
                .isInstanceOf(IllegalArgumentException.class);
    }

}