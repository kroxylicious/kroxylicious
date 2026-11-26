/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Arrays;
import java.util.Objects;

/**
 * Controls what the operator does when no {@code KafkaProxyNetworkPolicy} exists for a
 * {@code KafkaProxy}.
 *
 * <p>This is configured by the {@code KROXYLICIOUS_KPNP_ABSENT} environment variable.</p>
 */
public enum KpnpAbsent {

    /** Generate an allow-all {@code NetworkPolicy} for every target of a proxy with no policy. */
    ALLOW_ALL("allow-all"),

    /** Generate a deny-all {@code NetworkPolicy} for every target of a proxy with no policy. */
    DENY_ALL("deny-all"),

    /** Do not generate {@code NetworkPolicy} resources, and delete any previously generated ones. */
    NO_NP("no-np");

    private final String value;

    KpnpAbsent(String value) {
        this.value = value;
    }

    /**
     * @return the environment-variable value corresponding to this policy
     */
    public String value() {
        return value;
    }

    /**
     * Parses an environment-variable value.
     *
     * @param value the value to parse
     * @return the matching policy
     * @throws IllegalArgumentException if the value is not one of {@code allow-all},
     *         {@code deny-all} or {@code no-np}
     */
    static KpnpAbsent fromValue(String value) {
        Objects.requireNonNull(value);
        String trimmed = value.trim();
        return Arrays.stream(values())
                .filter(policy -> policy.value.equals(trimmed))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid value for " + OperatorMain.KROXYLICIOUS_KPNP_ABSENT_VAR_NAME + ": '" + value
                                + "'. Expected one of: allow-all, deny-all, no-np"));
    }
}
