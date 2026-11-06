/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import java.util.Objects;
import java.util.Set;

import io.kroxylicious.systemtests.api.FilterSpec;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@link FilterSpec} for the SASL-inspection filter.
 * Typically used alongside {@link EntityIsolationFilterSpec} or {@link SaslTerminationFilterSpec}
 * to extract authenticated principal information from SASL exchanges.
 *
 * @param mechanisms the SASL mechanism names to inspect (e.g. "SCRAM-SHA-512")
 */
public record SaslInspectionFilterSpec(@NonNull Set<String> mechanisms) implements FilterSpec {

    public SaslInspectionFilterSpec {
        Objects.requireNonNull(mechanisms, "mechanisms must not be null");
        mechanisms = Set.copyOf(mechanisms);
    }

    /**
     * Creates a spec that inspects SCRAM-SHA-512 (the most common configuration).
     *
     * @return a new spec for SCRAM-SHA-512 inspection
     */
    public static SaslInspectionFilterSpec scramSha512() {
        return new SaslInspectionFilterSpec(Set.of("SCRAM-SHA-512"));
    }
}