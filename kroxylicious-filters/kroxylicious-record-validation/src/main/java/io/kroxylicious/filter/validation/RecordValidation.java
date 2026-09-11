/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

import io.kroxylicious.filter.validation.config.ValidationConfig;
import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link FilterFactory} for {@link RecordValidationFilter}, which validates records
 * contained in produce requests before they are forwarded to the broker.
 */
@Plugin(configType = ValidationConfig.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.proxy.filter.validation.RecordValidation", since = "0.19.0")
public class RecordValidation implements FilterFactory<ValidationConfig, ValidationConfig> {

    /**
     * Creates a new factory instance, invoked by the plugin framework.
     */
    public RecordValidation() {
        // nothing to initialise: state is created in createFilter(FilterFactoryContext, ValidationConfig)
    }

    @Override
    public @NonNull ValidationConfig initialize(FilterFactoryContext context, @Nullable ValidationConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public RecordValidationFilter createFilter(FilterFactoryContext context, @NonNull ValidationConfig configuration) {
        ProduceRequestValidator validator = ProduceRequestValidatorBuilder.build(configuration);
        return new RecordValidationFilter(validator);
    }

}
