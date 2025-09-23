/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.HashSet;
import java.util.Set;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Config for the Sasl Initiation Filter.
 *
 * @param enabledMechanisms The enabled SASL mechanisms. Defaults to all supported mechanisms.
 */
public record Config(@NonNull Set<String> enabledMechanisms) {

    public Config(@Nullable Set<String> enabledMechanisms) {
        if (enabledMechanisms == null) {
            enabledMechanisms = Mech.SUPPORTED_MECHANISMS;
        }
        var unsupportedMechanisms = new HashSet<>(enabledMechanisms);
        unsupportedMechanisms.removeAll(Mech.SUPPORTED_MECHANISMS);
        if (!unsupportedMechanisms.isEmpty()) {
            throw new PluginConfigurationException("Unsupported SASL mechanisms: " + unsupportedMechanisms);
        }
        this.enabledMechanisms = enabledMechanisms;
    }
}
