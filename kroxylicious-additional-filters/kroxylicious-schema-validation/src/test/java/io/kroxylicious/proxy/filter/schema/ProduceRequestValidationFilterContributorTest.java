/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.schema.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceRequestValidationFilterContributorTest {
    private static final TypeReference<Map<String, Object>> MAP_REF = new TypeReference<>() {
    };

    @Test
    void testGetTypeName() {
        ProduceValidationFilter.Contributor contributor = new ProduceValidationFilter.Contributor();
        assertThat(contributor.getTypeName()).isEqualTo("ProduceValidator");
    }

    @Test
    void testGetInstance() {
        ProduceValidationFilter.Contributor contributor = new ProduceValidationFilter.Contributor();
        ValidationConfig config = new ValidationConfig(true, List.of(), new RecordValidationRule(null, null));
        Filter filter = contributor.getInstance(config, () -> () -> Executors.newScheduledThreadPool(1));
        assertThat(filter).isNotNull().isInstanceOf(ProduceValidationFilter.class);
    }

}