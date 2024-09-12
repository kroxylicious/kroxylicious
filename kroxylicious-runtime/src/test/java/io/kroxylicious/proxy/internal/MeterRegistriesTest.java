/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

class MeterRegistriesTest {

    @Test
    void testPreventingRegistrationOfMetersWithSameNameButDifferentTags() {
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        MeterRegistries.preventDifferentTagNameRegistration(registry);
        registry.counter("abc", List.of(Tag.of("a", "b")));
        registry.counter("abc", List.of(Tag.of("a", "c")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            registry.counter("abc", List.of(Tag.of("c", "d")));
        });
    }

}
