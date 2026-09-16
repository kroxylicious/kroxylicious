/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.List;

import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.protocol.types.BoundField;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NullableFieldEnumeratorTest {

    @Test
    void returnsNoFieldsWhenVersionHasNoNullableFields() {
        // Given
        JoinGroupRequestData instance = new JoinGroupRequestData();

        // When
        List<BoundField> fields = NullableFieldEnumerator.topLevelNullableFields(instance, (short) 0);

        // Then
        assertThat(fields).isEmpty();
    }

    @Test
    void returnsFieldIntroducedAsNullableAtVersion() {
        // Given
        JoinGroupRequestData instance = new JoinGroupRequestData();

        // When
        List<BoundField> fields = NullableFieldEnumerator.topLevelNullableFields(instance, (short) 5);

        // Then
        assertThat(fields).extracting(field -> field.def.name).containsExactly("group_instance_id");
    }

    @Test
    void returnsAllNullableFieldsPresentAtVersion() {
        // Given
        JoinGroupRequestData instance = new JoinGroupRequestData();

        // When
        List<BoundField> fields = NullableFieldEnumerator.topLevelNullableFields(instance, (short) 8);

        // Then
        assertThat(fields).extracting(field -> field.def.name).containsExactly("group_instance_id", "reason");
    }
}
