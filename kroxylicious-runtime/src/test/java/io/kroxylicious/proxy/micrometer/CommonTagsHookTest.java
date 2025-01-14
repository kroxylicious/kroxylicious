/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommonTagsHookTest {

    @Test
    void testNullConfig() {
        assertThatThrownBy(() -> {
            new CommonTagsHook().build(null);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCommonTags() {
        MicrometerConfigurationHook commonTagsHook = new CommonTagsHook().build(new CommonTagsHook.CommonTagsHookConfig(Map.of("a", "b")));
        MeterRegistry registry = givenRegistryConfiguredWith(commonTagsHook);
        Meter meter = whenCreateArbitraryMeter(registry);
        thenTagEquals(meter, "a", "b");
    }

    @Test
    void testNullCommonTagsMap() {
        MicrometerConfigurationHook commonTagsHook = new CommonTagsHook().build(new CommonTagsHook.CommonTagsHookConfig(null));
        MeterRegistry registry = givenRegistryConfiguredWith(commonTagsHook);
        Meter meter = whenCreateArbitraryMeter(registry);
        thenTagsEmpty(meter);
    }

    @Test
    void testEmptyCommonTagsMap() {
        MicrometerConfigurationHook commonTagsHook = new CommonTagsHook().build(new CommonTagsHook.CommonTagsHookConfig(new HashMap<>()));
        MeterRegistry registry = givenRegistryConfiguredWith(commonTagsHook);
        Meter meter = whenCreateArbitraryMeter(registry);
        thenTagsEmpty(meter);
    }

    private static void thenTagsEmpty(Meter counter) {
        Meter.Id id = counter.getId();
        assertThat(id.getTags()).isEmpty();
    }

    private static void thenTagEquals(Meter counter, String key, String value) {
        Meter.Id id = counter.getId();
        String tag = id.getTag(key);
        assertThat(tag).isNotNull().isEqualTo(value);
    }

    private static Meter whenCreateArbitraryMeter(MeterRegistry registry) {
        return registry.counter(UUID.randomUUID().toString());
    }

    private static MeterRegistry givenRegistryConfiguredWith(MicrometerConfigurationHook hook) {
        MeterRegistry registry = new CompositeMeterRegistry();
        hook.configure(registry);
        return registry;
    }

}
