/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.schema.config.BytebufValidation;
import io.kroxylicious.proxy.filter.schema.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.schema.config.SyntacticallyCorrectJsonConfig;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceRequestValidationFilterContributorTest {

    private final ProduceRequestValidationFilterContributor contributor = new ProduceRequestValidationFilterContributor();

    @Test
    void testGetConfigType() {
        Class<? extends BaseConfig> configType = contributor.getConfigType("ProduceValidator");
        assertThat(configType).isEqualTo(ValidationConfig.class);
    }

    @Test
    void testGetInstance() {
        BytebufValidation validation = new BytebufValidation(new SyntacticallyCorrectJsonConfig(false), true, true);
        ValidationConfig config = new ValidationConfig(true, List.of(), new RecordValidationRule(validation, validation));
        KrpcFilter filter = contributor.getInstance("ProduceValidator", config, Mockito.mock(FilterContributorContext.class));
        assertThat(filter).isInstanceOf(ProduceValidationFilter.class);
    }

}