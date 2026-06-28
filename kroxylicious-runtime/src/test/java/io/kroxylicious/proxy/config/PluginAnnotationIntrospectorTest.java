/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.ClassNameIdResolver;

import io.kroxylicious.proxy.plugin.PluginImplConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginAnnotationIntrospectorTest {

    private static PluginImplConfig mockPluginImplConfig(String implName) {
        var conf1 = mock(PluginImplConfig.class);
        when(conf1.implNameProperty()).thenReturn(implName);
        return conf1;
    }

    @SuppressWarnings("StringOperationCanBeSimplified") // Intentionally avoid using same string instance
    @Test
    void equalsForFakeJsonTypeInfo() {
        String implName = "ImplName";
        var conf1 = mockPluginImplConfig(implName);
        var conf2 = mockPluginImplConfig(new String(implName));
        var conf3 = mockPluginImplConfig("Not" + implName);
        var info1 = new PluginAnnotationIntrospector.FakeJsonTypeInfo(conf1);
        var info2 = new PluginAnnotationIntrospector.FakeJsonTypeInfo(conf2);
        var info3 = new PluginAnnotationIntrospector.FakeJsonTypeInfo(conf3);
        assertThat(info1).isEqualTo(info2)
                .isNotEqualTo(info3);
        assertThat(info1).hasSameHashCodeAs(info2)
                .doesNotHaveSameHashCodeAs(info3);
    }

    @Test
    void equalsForFakeJsonTypeIdResolver() {
        var resolver1 = new PluginAnnotationIntrospector.FakeJsonTypeIdResolver();
        var resolver2 = new PluginAnnotationIntrospector.FakeJsonTypeIdResolver();
        var resolver3 = mock(JsonTypeIdResolver.class);
        when(resolver3.value()).thenReturn((Class) ClassNameIdResolver.class);
        assertThat(resolver1).isEqualTo(resolver2)
                .isNotEqualTo(resolver3);
        assertThat(resolver1).hasSameHashCodeAs(resolver2)
                .doesNotHaveSameHashCodeAs(resolver3);
    }

}