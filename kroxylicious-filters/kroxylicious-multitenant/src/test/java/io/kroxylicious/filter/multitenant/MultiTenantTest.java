/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.multitenant;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.test.schema.SchemaValidationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class MultiTenantTest {

    @Test
    void shouldHaveLegacyAndConfig2PluginAnnotations() {
        Plugin[] annotations = MultiTenant.class.getAnnotationsByType(Plugin.class);

        assertThat(annotations).hasSize(2);

        var versionToConfigType = java.util.Arrays.stream(annotations)
                .collect(java.util.stream.Collectors.toMap(Plugin::configVersion, Plugin::configType));

        assertThat(versionToConfigType).containsOnly(
                entry("", MultiTenantConfig.class),
                entry("v1alpha1", MultiTenantConfig.class));
    }

    @Test
    void fullConfigShouldPassSchemaValidation() {
        // Config with all fields populated that Java accepts
        new MultiTenantConfig("_");

        // Same config in raw YAML form — must also pass schema validation
        SchemaValidationAssert.assertSchemaAccepts("MultiTenant", "v1alpha1", Map.of(
                "prefixResourceNameSeparator", "_"));
    }

    @Test
    void createFilter() {
        var factory = new MultiTenant();
        Filter filter = factory.createFilter(Mockito.mock(FilterFactoryContext.class), Mockito.mock(MultiTenantConfig.class));
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantFilter.class);
    }

}
