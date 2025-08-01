/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import javax.annotation.Nullable;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = ValidationConfig.class)
public class RecordValidation implements FilterFactory<ValidationConfig, ValidationConfig> {

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
