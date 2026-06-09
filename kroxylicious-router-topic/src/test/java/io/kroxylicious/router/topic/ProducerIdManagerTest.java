/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.kroxylicious.router.topic.ProducerIdManager.ProducerIdEpoch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ProducerIdManager}.
 */
class ProducerIdManagerTest {

    private static final Duration TTL = Duration.ofSeconds(60);
    private static final long TTL_NANOS = TTL.toNanos();

    private final AtomicLong clock = new AtomicLong(0);

    private ProducerIdManager createManager() {
        return new ProducerIdManager(TTL, clock::get);
    }

    private Map<String, ProducerIdEpoch> sampleMapping() {
        return Map.of(
                "route-a", new ProducerIdEpoch(100L, (short) 0),
                "route-b", new ProducerIdEpoch(200L, (short) 0));
    }

    @Test
    void shouldStoreAndRetrieveMapping() {
        var manager = createManager();
        manager.put(1L, sampleMapping());

        var result = manager.get(1L);
        assertThat(result).isNotNull();
        assertThat(result).containsEntry("route-a", new ProducerIdEpoch(100L, (short) 0));
        assertThat(result).containsEntry("route-b", new ProducerIdEpoch(200L, (short) 0));
    }

    @Test
    void shouldReturnNullForUnknownPid() {
        var manager = createManager();

        assertThat(manager.get(999L)).isNull();
    }

    @Test
    void shouldEvictExpiredEntries() {
        var manager = createManager();
        manager.put(1L, sampleMapping());
        assertThat(manager.size()).isEqualTo(1);

        clock.set(TTL_NANOS + 1);
        manager.evictExpired();

        assertThat(manager.get(1L)).isNull();
        assertThat(manager.size()).isEqualTo(0);
    }

    @Test
    void shouldNotEvictFreshEntries() {
        var manager = createManager();
        manager.put(1L, sampleMapping());

        clock.set(TTL_NANOS - 1);
        manager.evictExpired();

        assertThat(manager.get(1L)).isNotNull();
        assertThat(manager.size()).isEqualTo(1);
    }

    @Test
    void shouldResetTtlOnGet() {
        var manager = createManager();
        manager.put(1L, sampleMapping());

        clock.set(TTL_NANOS - 1);
        assertThat(manager.get(1L)).isNotNull();

        clock.set(TTL_NANOS + 1);
        manager.evictExpired();

        assertThat(manager.get(1L)).as("entry should survive because get reset the TTL").isNotNull();
    }

    @Test
    void shouldEvictSelectivelyByAge() {
        var manager = createManager();

        clock.set(0);
        manager.put(1L, Map.of("route-a", new ProducerIdEpoch(100L, (short) 0)));

        clock.set(TTL_NANOS / 2);
        manager.put(2L, Map.of("route-a", new ProducerIdEpoch(200L, (short) 0)));

        clock.set(TTL_NANOS + 1);
        manager.evictExpired();

        assertThat(manager.get(1L)).as("older entry should be evicted").isNull();
        assertThat(manager.get(2L)).as("newer entry should survive").isNotNull();
        assertThat(manager.size()).isEqualTo(1);
    }

    @Test
    void updateRouteEpochShouldUpdateSpecificRoute() {
        var manager = createManager();
        manager.put(1L, new java.util.HashMap<>(sampleMapping()));

        manager.updateRouteEpoch(1L, "route-a",
                new ProducerIdEpoch(100L, (short) 5));

        var result = manager.get(1L);
        assertThat(result).isNotNull();
        assertThat(result.get("route-a"))
                .isEqualTo(new ProducerIdEpoch(100L, (short) 5));
        assertThat(result.get("route-b"))
                .isEqualTo(new ProducerIdEpoch(200L, (short) 0));
    }

    @Test
    void updateRouteEpochShouldNoopForUnknownPid() {
        var manager = createManager();

        manager.updateRouteEpoch(999L, "route-a",
                new ProducerIdEpoch(100L, (short) 1));

        assertThat(manager.get(999L)).isNull();
    }

    @Test
    void shouldReportSize() {
        var manager = createManager();
        assertThat(manager.size()).isEqualTo(0);

        manager.put(1L, sampleMapping());
        assertThat(manager.size()).isEqualTo(1);

        manager.put(2L, sampleMapping());
        assertThat(manager.size()).isEqualTo(2);
    }
}
