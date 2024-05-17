/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An expression involving a single label key within a {@link Selector}.
 * @param key The label key
 * @param operator The operator
 * @param values The values for the operator. This must be null iff the operator is
 * {@link MatchOperator#EXISTS} or {@link MatchOperator#NOT_EXISTS}.
 */
@JsonPropertyOrder({ "key", "operator", "values" })
public record MatchExpression(
                              @JsonProperty(value = "key", required = true) @NonNull String key,
                              @JsonProperty(value = "operator", required = true) @NonNull MatchOperator operator,
                              @JsonProperty("values") @Nullable Set<String> values)
        implements Predicate<Map<String, String>> {

    /**
     * Factory method for Exists expressions
     * @param key The key
     * @return The expression
     */
    @NonNull
    public static MatchExpression exists(@NonNull String key) {
        return new MatchExpression(key, MatchOperator.EXISTS, null);
    }

    /**
     * Factory method for DoesNotExist expressions
     * @param key The key
     * @return The expression
     */
    @NonNull
    public static MatchExpression notExists(@NonNull String key) {
        return new MatchExpression(key, MatchOperator.NOT_EXISTS, null);
    }

    /**
     * Factory method for In expressions
     * @param key The key
     * @return The expression
     */
    @NonNull
    public static MatchExpression in(@NonNull String key, @NonNull Set<String> values) {
        return new MatchExpression(key, MatchOperator.IN, values);
    }

    /**
     * Factory method for NotIn expressions
     * @param key The key
     * @return The expression
     */
    @NonNull
    public static MatchExpression notIn(@NonNull String key, @NonNull Set<String> values) {
        return new MatchExpression(key, MatchOperator.NOT_IN, values);
    }

    public MatchExpression {
        Labels.validateLabelKey(key);
        Objects.requireNonNull(operator);
        if ((operator == MatchOperator.EXISTS
                || operator == MatchOperator.NOT_EXISTS) && values != null) {
            throw new IllegalArgumentException();
        }
        if ((operator == MatchOperator.IN
                || operator == MatchOperator.NOT_IN)) {
            Objects.requireNonNull(values);
            values.forEach(Labels::validateLabelValue);
        }
    }

    /**
     * Returns the string form of this expression, for example "foo in (x,y)", "foo==x" (for an {@link MatchOperator#IN} expression with a single value) or "foo" (for an {@link MatchOperator#EXISTS} expression).
     * @return the string form of this expression.
     */
    @Override
    public String toString() {
        return switch (operator()) {
            case EXISTS -> key();
            case NOT_EXISTS -> "!" + key();
            case IN -> values().size() == 1 ? key() + "==" + values().iterator().next()
                    : key() + " in " + values().stream().sorted().collect(Collectors.joining(",", "(", ")"));
            case NOT_IN -> values().size() == 1 ? key() + "!=" + values().iterator().next()
                    : key() + " notin " + values().stream().sorted().collect(Collectors.joining(",", "(", ")"));
        };
    }

    /**
     * Test whether this expression matches the given labels.
     * @param labels the input argument
     * @return true iff this expression matches the given labels
     */
    @Override
    public boolean test(Map<String, String> labels) {
        return switch (operator) {
            case EXISTS -> labels.containsKey(key());
            case NOT_EXISTS -> !labels.containsKey(key());
            case IN -> labels.containsKey(key()) && values.contains(labels.get(key()));
            case NOT_IN -> !labels.containsKey(key()) || !values.contains(labels.get(key()));
        };
    }

    /**
     * Returns true if this expression is disjoint from the given other expression.
     * Two expressions are disjoint if the set of labels selected by each are disjoint (for all possible labels).
     * Disjointness is an irreflexive, symmetric, binary relation on expressions.
     * @param other the other expression
     * @return true if this expression is disjoint from the given other expression.
     */
    public boolean disjoint(MatchExpression other) {
        if (this.operator().compareTo(other.operator()) > 0) {
            // ensure symmetry
            return other.disjoint(this);
        }
        // "foo" and "foo" are never disjoint (they're ==), "foo" and "bar" are not always disjoint (e.g. "foo=a,bar=a" would be selected by both)
        if (this.operator() == MatchOperator.EXISTS
                && other.operator() == MatchOperator.NOT_EXISTS) {
            return this.key().equals(other.key()); // foo and !foo are disjoint
        }
        // "foo" and "foo in X" are never disjoint (they're ==), "foo" and "bar in X" are not always disjoint (e.g. "foo=a,bar=x" would be selected by both)

        // foo, foo not in A is not always disjoint (e.g. "foo=z" would be selected by both)

        // "!foo" and "!foo" are never disjoint (they're ==), "!foo" and "!bar" are not always disjoint (e.g. "baz=a" would be selected by both)
        if (this.operator() == MatchOperator.NOT_EXISTS
                && other.operator() == MatchOperator.IN) {
            return this.key().equals(other.key()); // "!foo" and "foo in X" are disjoint because a result in the latter cannot be a result in the former
        }
        // "!foo" and "foo notin X" are not always disjoint (e.g. "baz=a" would be selected by both)

        else if (this.operator() == MatchOperator.IN
                && other.operator() == MatchOperator.IN) {
            return this.key().equals(other.key())
                    && disjointSets(this.values(), other.values()); // foo in A and foo in B are disjoint (for disjoint sets A, B)
        }

        else if (this.operator() == MatchOperator.IN
                && other.operator() == MatchOperator.NOT_IN) {
            return this.key().equals(other.key())
                    && isSubset(this.values, other.values()); // foo in {x,y}, foo notin {y, z} only if {x, y} is a subset of {y, z}
        }

        return false;
    }

    static <T> boolean isSubset(Set<T> s, Set<T> s2) {
        return s2.containsAll(s);
    }

    private static <T> boolean disjointSets(Set<T> s, Set<T> s2) {
        for (var e : s) {
            if (s2.contains(e)) {
                return false;
            }
        }
        return true;
    }
}
