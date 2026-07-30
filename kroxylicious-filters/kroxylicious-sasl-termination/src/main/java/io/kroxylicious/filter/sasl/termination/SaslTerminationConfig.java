/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the SASL termination filter.
 *
 * @param mechanisms list of mechanism configurations
 * @param maxTimeBeforeReauth maximum session lifetime before reauthentication is required (KIP-368), null = disabled
 */
public record SaslTerminationConfig(
                                    @JsonProperty(required = true) List<MechanismConfig> mechanisms,
                                    @Nullable Duration maxTimeBeforeReauth) {

    public SaslTerminationConfig {
        if (mechanisms == null || mechanisms.isEmpty()) {
            throw new IllegalArgumentException("At least one mechanism must be configured");
        }

        Set<String> seen = new HashSet<>();
        for (MechanismConfig mechanism : mechanisms) {
            if (!seen.add(mechanism.mechanismName())) {
                throw new IllegalArgumentException(
                        "Duplicate mechanism: " + mechanism.mechanismName());
            }
        }
    }
}
