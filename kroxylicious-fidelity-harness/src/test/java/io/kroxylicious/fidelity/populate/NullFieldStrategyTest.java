/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NullFieldStrategyTest {

    private static final Field CLIENT_ID_FIELD = new Field("client_id", Type.NULLABLE_STRING, "doc");
    private static final BoundField TARGET_FIELD = new Schema(CLIENT_ID_FIELD).get("client_id");
    private static final BoundField OTHER_FIELD = new Schema(new Field("mechanism", Type.STRING, "doc")).get("mechanism");

    @Test
    void shouldResolveTargetFieldToNull() {
        // Given
        NullFieldStrategy strategy = new NullFieldStrategy(TARGET_FIELD, field -> new FieldDecision.Value("should not be used"));

        // When
        FieldDecision decision = strategy.resolve(TARGET_FIELD);

        // Then
        assertThat(decision).isEqualTo(new FieldDecision.Value(null));
    }

    @Test
    void shouldResolveNullForSameField() {
        // Given
        BoundField sameFieldDifferentWrapper = new Schema(TARGET_FIELD.def).get("client_id");
        NullFieldStrategy strategy = new NullFieldStrategy(TARGET_FIELD, field -> new FieldDecision.Value("should not be used"));

        // When
        FieldDecision decision = strategy.resolve(sameFieldDifferentWrapper);

        // Then
        assertThat(decision).isEqualTo(new FieldDecision.Value(null));
    }

    @Test
    void shouldDelegateNonTargetFieldToWrappedStrategy() {
        // Given
        FieldDecision delegateDecision = new FieldDecision.Value("delegated value");
        NullFieldStrategy strategy = new NullFieldStrategy(TARGET_FIELD, field -> delegateDecision);

        // When
        FieldDecision decision = strategy.resolve(OTHER_FIELD);

        // Then
        assertThat(decision).isSameAs(delegateDecision);
    }
}
