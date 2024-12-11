/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.validation.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordValidationTest {

    @Test
    void initRejectsMissingConfig() {
        RecordValidation factory = new RecordValidation();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage(RecordValidation.class.getSimpleName() + " requires configuration, but config object is null");
    }

    @Test
    void shouldInitAndCreateFilter() {
        RecordValidation factory = new RecordValidation();
        var config = factory.initialize(null, new ValidationConfig(List.of(), new RecordValidationRule(null, null)));
        Filter filter = factory.createFilter(null, config);
        assertThat(filter).isNotNull().isInstanceOf(RecordValidationFilter.class);
    }

}
