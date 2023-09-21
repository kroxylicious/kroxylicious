/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.util.List;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.schema.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;
import io.kroxylicious.proxy.service.ConfigurationDefinition;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceRequestValidationFilterContributorTest {

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        ProduceValidationFilter.Contributor contributor = new ProduceValidationFilter.Contributor();
        ConfigurationDefinition actualConfigurationDefinition = contributor.getConfigDefinition();
        assertThat(actualConfigurationDefinition).isNotNull().hasFieldOrPropertyWithValue("configurationType", ValidationConfig.class);
    }

    @Test
    void testGetInstance() {
        ProduceValidationFilter.Contributor contributor = new ProduceValidationFilter.Contributor();
        ValidationConfig config = new ValidationConfig(true, List.of(), new RecordValidationRule(null, null));
        Filter filter = contributor.getInstance(FilterConstructContext.wrap(config, () -> Executors.newScheduledThreadPool(1)));
        assertThat(filter).isNotNull().isInstanceOf(ProduceValidationFilter.class);
    }

}