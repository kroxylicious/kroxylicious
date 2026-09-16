/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import org.apache.kafka.common.protocol.types.BoundField;

/**
 * Nulls one specific field, identified by reference, and delegates every other field to a wrapped
 * strategy.
 * <p>
 * Matching by reference rather than by name sidesteps the fact that {@link BoundField} carries no
 * ancestry: {@link NullableFieldEnumerator} hands back the actual {@link BoundField} instances from
 * Kafka's static, per-version {@code Schema}, and the same instances are produced again when
 * {@link SchemaDrivenMessagePopulator} walks that schema to populate a message, so identity is a
 * reliable way to target the exact field that was enumerated.
 */
public final class NullFieldStrategy implements FieldPopulationStrategy {

    private final BoundField target;
    private final FieldPopulationStrategy delegate;

    /**
     * Construct the strategy.
     *
     * @param target the field to null out
     * @param delegate the strategy to use for every other field
     */
    public NullFieldStrategy(BoundField target, FieldPopulationStrategy delegate) {
        this.target = target;
        this.delegate = delegate;
    }

    @Override
    @SuppressWarnings("ReferenceEquality") // BoundField declares no equals()/hashCode(); identity is the only, and intended, notion of equality here.
    public FieldDecision resolve(BoundField field) {
        return field == target ? new FieldDecision.Value(null) : delegate.resolve(field);
    }
}
