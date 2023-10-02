/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;
import io.kroxylicious.proxy.filter.schema.validation.request.ProduceRequestValidator;

public class ProduceValidationFilterFactory implements FilterFactory<ProduceValidationFilter, ValidationConfig> {

    @Override
    public Class<ProduceValidationFilter> filterType() {
        return ProduceValidationFilter.class;
    }

    @Override
    public Class<ValidationConfig> configType() {
        return ValidationConfig.class;
    }

    @Override
    public ProduceValidationFilter createFilter(FilterCreationContext context, ValidationConfig configuration) {
        ProduceRequestValidator validator = ProduceValidationFilterBuilder.build(configuration);
        return new ProduceValidationFilter(configuration.isForwardPartialRequests(), validator);
    }
}
