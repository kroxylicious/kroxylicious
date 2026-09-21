/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;

/**
 * Nulls one specific field, identified by its underlying {@link Field}, and delegates every other field
 * to a wrapped strategy.
 * <p>
 * {@link BoundField} carries no {@code equals()}/{@code hashCode()}, and {@link SchemaDrivenMessagePopulator}
 * allocates a fresh {@link BoundField} wrapper on every {@code expandFields()} call, so matching by
 * {@link BoundField} identity does not reliably identify the field {@link NullableFieldEnumerator}
 * enumerated. The wrapped {@link Field}, however, is the same static, per-version instance every time, so
 * matching on {@code field.def} identity is the reliable way to target the exact field that was enumerated.
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
    public FieldDecision resolve(BoundField field) {
        return isSameField(field, target) ? new FieldDecision.Value(null) : delegate.resolve(field);
    }

    @SuppressWarnings("ReferenceEquality") // Field declares no equals()/hashCode(); identity is the only, and intended, notion of equality here.
    private static boolean isSameField(BoundField field, BoundField target) {
        return field.def == target.def;
    }
}
