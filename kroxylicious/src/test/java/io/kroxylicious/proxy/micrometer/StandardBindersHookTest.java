/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StandardBindersHookTest {

    private static final String AUTO_CLOSEABLE_BINDER = "AutoCloseableBinder";
    @Mock(extraInterfaces = AutoCloseable.class)
    MeterBinder closeableBinder;

    @Test
    public void testNullHookConfigThrows() {
        assertThatThrownBy(() -> new StandardBindersHook(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNullBinderNames() {
        try (StandardBindersHook hook = new StandardBindersHook(new StandardBindersHook.StandardBindersHookConfig(null))) {
            MeterRegistry registry = whenRegistryConfiguredWith(hook);
            thenNoMetersRegistered(registry);
        }
    }

    @Test
    public void testEmptyBinderNames() {
        try (StandardBindersHook hook = new StandardBindersHook(new StandardBindersHook.StandardBindersHookConfig(List.of()))) {
            MeterRegistry registry = whenRegistryConfiguredWith(hook);
            thenNoMetersRegistered(registry);
        }
    }

    @Test
    public void testKnownBinder() {
        try (StandardBindersHook hook = new StandardBindersHook(new StandardBindersHook.StandardBindersHookConfig(List.of("UptimeMetrics")))) {
            MeterRegistry registry = whenRegistryConfiguredWith(hook);
            thenUptimeMeterRegistered(registry);
        }
    }

    @Test
    public void testUnknownBinder() {
        try (StandardBindersHook hook = new StandardBindersHook(new StandardBindersHook.StandardBindersHookConfig(List.of("SadClown")))) {
            assertThatThrownBy(() -> whenRegistryConfiguredWith(hook)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testAutoCloseableBindingClosed() throws Exception {
        var hook = new StandardBindersHook(new StandardBindersHook.StandardBindersHookConfig(List.of(AUTO_CLOSEABLE_BINDER))) {
            @Override
            protected MeterBinder getBinder(String binderName) {
                if (binderName.equals(AUTO_CLOSEABLE_BINDER)) {
                    return closeableBinder;
                }
                throw new IllegalArgumentException();
            }
        };
        var registry = whenRegistryConfiguredWith(hook);
        verify(closeableBinder).bindTo(registry);

        hook.close();
        verify(((AutoCloseable) closeableBinder)).close();
    }

    private static void thenUptimeMeterRegistered(MeterRegistry registry) {
        Meter meter = registry.get("process.uptime").meter();
        assertThat(meter).describedAs("uptime gauge").isNotNull();
    }

    private static void thenNoMetersRegistered(MeterRegistry registry) {
        assertThat(registry.getMeters()).isEmpty();
    }

    private static MeterRegistry whenRegistryConfiguredWith(MicrometerConfigurationHook hook) {
        MeterRegistry registry = new CompositeMeterRegistry();
        hook.configure(registry);
        return registry;
    }

}