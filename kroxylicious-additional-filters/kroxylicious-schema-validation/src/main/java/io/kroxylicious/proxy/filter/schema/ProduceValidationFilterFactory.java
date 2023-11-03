/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;
import io.kroxylicious.proxy.filter.schema.validation.request.ProduceRequestValidator;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = ValidationConfig.class)
public class ProduceValidationFilterFactory implements FilterFactory<ValidationConfig, ValidationConfig> {

    @Override
    public ValidationConfig initialize(FilterFactoryContext context, ValidationConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public ProduceValidationFilter createFilter(FilterFactoryContext context, ValidationConfig configuration) {
        ProduceRequestValidator validator = ProduceValidationFilterBuilder.build(configuration);
        return new ProduceValidationFilter(configuration.isForwardPartialRequests(), validator);
    }

}
