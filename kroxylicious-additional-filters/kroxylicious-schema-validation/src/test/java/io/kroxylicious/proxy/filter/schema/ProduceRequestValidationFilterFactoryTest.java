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
import io.kroxylicious.proxy.filter.InvalidFilterConfigurationException;
import io.kroxylicious.proxy.filter.schema.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProduceRequestValidationFilterFactoryTest {

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        ProduceValidationFilter.Factory factory = new ProduceValidationFilter.Factory();
        assertThat(factory.configType()).isEqualTo(ValidationConfig.class);
        assertThatThrownBy(() -> factory.validateConfiguration(null)).isInstanceOf(InvalidFilterConfigurationException.class)
                .hasMessage("ProduceValidationFilter requires configuration, but config object is null");
    }

    @Test
    void testGetInstance() {
        ProduceValidationFilter.Factory factory = new ProduceValidationFilter.Factory();
        ValidationConfig config = new ValidationConfig(true, List.of(), new RecordValidationRule(null, null));
        Filter filter = factory.createFilter(() -> Executors.newScheduledThreadPool(1), config);
        assertThat(filter).isNotNull().isInstanceOf(ProduceValidationFilter.class);
    }

}
