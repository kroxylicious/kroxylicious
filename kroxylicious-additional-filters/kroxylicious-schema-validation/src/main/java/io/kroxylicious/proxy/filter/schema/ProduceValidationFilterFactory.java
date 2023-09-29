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

public class ProduceValidationFilterFactory extends FilterFactory<ProduceValidationFilter, ValidationConfig> {

    public ProduceValidationFilterFactory() {
        super(ValidationConfig.class, ProduceValidationFilter.class);
    }

    @Override
    public ProduceValidationFilter createFilter(FilterCreationContext context, ValidationConfig configuration) {
        ProduceRequestValidator validator = ProduceValidationFilterBuilder.build(configuration);
        return new ProduceValidationFilter(configuration.isForwardPartialRequests(), validator);
    }
}
