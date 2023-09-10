/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.schema;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;
import io.kroxylicious.proxy.filter.schema.validation.request.ProduceRequestValidator;
import io.kroxylicious.proxy.service.BaseContributor;

/**
 * Contributor for request validation filters
 */
public class ProduceRequestValidationFilterContributor extends BaseContributor<Filter, FilterConstructContext> implements FilterContributor {

    private static final BaseContributorBuilder<Filter, FilterConstructContext> FILTERS = BaseContributor.<Filter, FilterConstructContext> builder()
            .add("ProduceValidator", ValidationConfig.class, (config) -> {
                ProduceRequestValidator validator = ProduceValidationFilterBuilder.build(config);
                return new ProduceValidationFilter(config.isForwardPartialRequests(), validator);
            });

    /**
     * Constructor (called via ${@link java.util.ServiceLoader})
     */
    public ProduceRequestValidationFilterContributor() {
        super(FILTERS);
    }
}
