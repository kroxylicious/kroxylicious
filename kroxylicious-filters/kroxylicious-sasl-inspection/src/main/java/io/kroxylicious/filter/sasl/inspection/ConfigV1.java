/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.inspection;

import java.util.Set;
import java.util.stream.Stream;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Config2 (versioned) configuration for the {@link SaslInspection} filter.
 * References a {@link SaslSubjectBuilderService} plugin instance by name
 * rather than embedding its configuration inline.
 *
 * @param enabledMechanisms set of SASL mechanisms to enable, or null for auto-detection
 * @param subjectBuilder optional name of a SaslSubjectBuilderService plugin instance
 * @param requireAuthentication whether successful authentication is required before forwarding requests
 */
public record ConfigV1(
                       @Nullable Set<String> enabledMechanisms,
                       @Nullable String subjectBuilder,
                       @Nullable Boolean requireAuthentication)
        implements HasPluginReferences {

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        return subjectBuilder == null
                ? Stream.empty()
                : Stream.of(new PluginReference<>(SaslSubjectBuilderService.class.getName(), subjectBuilder));
    }
}
