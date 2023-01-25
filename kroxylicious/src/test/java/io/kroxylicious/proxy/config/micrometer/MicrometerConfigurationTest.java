/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.micrometer;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MicrometerConfigurationTest {

    public static class TestHook implements MicrometerConfigurationHook {
        @Override
        public void configure(MeterRegistry targetRegistry) {

        }
    }

    @Test
    public void testLocatesJvmBinder() {
        MicrometerConfiguration config = new MicrometerConfiguration(List.of("JvmGcMetrics"), null, null);
        List<Class<? extends MeterBinder>> binders = config.getBinders();
        assertThat(binders).contains(JvmGcMetrics.class);
    }

    @Test
    public void testNullBinderList() {
        MicrometerConfiguration config = new MicrometerConfiguration(null, null, null);
        List<Class<? extends MeterBinder>> binders = config.getBinders();
        assertThat(binders).isEmpty();
    }

    @Test
    public void testNonExistentBinder() {
        assertThrows(IllegalArgumentException.class, () -> {
            new MicrometerConfiguration(List.of("non.existent.Binder"), null, null);
        });
    }

    @Test
    public void testEmptyBinderList() {
        MicrometerConfiguration config = new MicrometerConfiguration(List.of(), null, null);
        List<Class<? extends MeterBinder>> binders = config.getBinders();
        assertThat(binders).isEmpty();
    }

    @Test
    public void testLocatesMicrometerHook() {
        MicrometerConfiguration config = new MicrometerConfiguration(null, TestHook.class.getTypeName(), null);
        Optional<Class<? extends MicrometerConfigurationHook>> hookClass = config.loadConfigurationHookClass();
        assertTrue(hookClass.isPresent(), "configuration hook class optional should be present");
        assertEquals(TestHook.class, hookClass.get());
    }

    @Test
    public void testNonExistentMicrometerHook() {
        assertThrows(RuntimeException.class, () -> {
            new MicrometerConfiguration(null, "bad.class.DoesntExist", null);
        });
    }

    @Test
    public void testConfigHookEmptyIfNoClassnameProvided() {
        MicrometerConfiguration config = new MicrometerConfiguration(null, null, null);
        Optional<Class<? extends MicrometerConfigurationHook>> hookClass = config.loadConfigurationHookClass();
        assertThat(hookClass).describedAs("configuration hook class optional").isEmpty();
    }

    @Test
    public void testLocatesSystemBinder() {
        MicrometerConfiguration config = new MicrometerConfiguration(List.of("JvmGcMetrics"), null, null);
        List<Class<? extends MeterBinder>> binders = config.getBinders();
        assertThat(binders).contains(JvmGcMetrics.class);
    }

    @Test
    public void testLocatesBinderUsingFullPackage() {
        MicrometerConfiguration config = new MicrometerConfiguration(List.of("io.micrometer.core.instrument.binder.system.UptimeMetrics"), null, null);
        List<Class<? extends MeterBinder>> binders = config.getBinders();
        assertThat(binders).contains(UptimeMetrics.class);
    }

    @Test
    public void testNullCommonTags() {
        MicrometerConfiguration config = new MicrometerConfiguration(null, null, null);
        assertThat(config.getCommonTags()).isEmpty();
    }

    @Test
    public void testEmptyCommonTags() {
        MicrometerConfiguration config = new MicrometerConfiguration(null, null, Map.of());
        assertThat(config.getCommonTags()).isEmpty();
    }

    @Test
    public void testNonEmptyCommonTags() {
        MicrometerConfiguration config = new MicrometerConfiguration(null, null, Map.of("a", "b"));
        assertThat(config.getCommonTags()).contains(Tag.of("a", "b"));
    }

}
