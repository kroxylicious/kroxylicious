/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = ValidationConfig.class)
public class RecordValidation implements FilterFactory<ValidationConfig, ValidationConfig> {

    @Override
    public ValidationConfig initialize(FilterFactoryContext context, ValidationConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public RecordValidationFilter createFilter(FilterFactoryContext context, ValidationConfig configuration) {
        ProduceRequestValidator validator = ProduceRequestValidatorBuilder.build(configuration);
        return new RecordValidationFilter(validator);
    }

}
