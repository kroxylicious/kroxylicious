/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.schema.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProduceValidationTest {

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        ProduceValidation factory = new ProduceValidation();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage("ProduceValidation requires configuration, but config object is null");
    }

    @Test
    void testGetInstance() {
        ProduceValidation factory = new ProduceValidation();
        ValidationConfig config = new ValidationConfig(true, List.of(), new RecordValidationRule(null, null));
        Filter filter = factory.createFilter(null, config);
        assertThat(filter).isNotNull().isInstanceOf(ProduceValidation.Filter.class);
    }

}
