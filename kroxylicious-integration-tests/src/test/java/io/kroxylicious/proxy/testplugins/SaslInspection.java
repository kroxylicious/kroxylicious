/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = SaslInspection.Config.class)
public class SaslInspection implements FilterFactory<SaslInspection.Config, SaslInspection.Config> {
    /**
     * @param announceAuthResults Whether the filter should announce authentications via
     * {@link io.kroxylicious.proxy.filter.FilterContext#clientSaslAuthenticationSuccess(String, String)}
     * and {@link io.kroxylicious.proxy.filter.FilterContext#clientSaslAuthenticationFailure(String, String, Exception)}.
     * \Default is true.
     * @param enabledMechanisms The enabled SASL mechanisms. Defaults to all supported mechanisms.
     */
    public record Config(
                         boolean announceAuthResults,
                         @NonNull Set<String> enabledMechanisms) {

        @JsonCreator
        public Config(@Nullable Boolean announceAuthResults,
                      @Nullable Set<String> enabledMechanisms) {
            this(announceAuthResults == null || announceAuthResults, enabledMechanisms);
        }

        public Config(boolean announceAuthResults,
                      @Nullable Set<String> enabledMechanisms) {
            if (enabledMechanisms == null) {
                enabledMechanisms = SaslInspectionFilter.SUPPORTED_MECHANISMS;
            }
            var unsupportedMechanisms = new HashSet<>(enabledMechanisms);
            unsupportedMechanisms.removeAll(SaslInspectionFilter.SUPPORTED_MECHANISMS);
            if (!unsupportedMechanisms.isEmpty()) {
                throw new IllegalConfigurationException("Unsupported SASL mechanisms: " + unsupportedMechanisms);
            }
            this.announceAuthResults = announceAuthResults;
            this.enabledMechanisms = enabledMechanisms;
        }
    }

    @Override
    public Config initialize(FilterFactoryContext context,
                             @Nullable Config config)
            throws PluginConfigurationException {
        return config == null ? new Config(null, null) : config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        Objects.requireNonNull(config);
        return new SaslInspectionFilter(config);
    }
}
