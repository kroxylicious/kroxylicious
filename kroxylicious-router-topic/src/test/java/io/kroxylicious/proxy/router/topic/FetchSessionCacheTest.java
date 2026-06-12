/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class FetchSessionCacheTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    {
        Metrics.globalRegistry.add(registry);
    }

    @AfterEach
    void tearDown() {
        Metrics.globalRegistry.remove(registry);
        registry.close();
    }

    private static FetchSessionCache createCache(int maxSlots, long minEvictionMs) {
        return new FetchSessionCache(maxSlots, minEvictionMs, "testVc", "testRouter");
    }

    @Test
    void shouldTagMetricsWithVirtualClusterAndRouter() {
        createCache(10, 0);

        var sessions = registry.find(FetchSessionCache.ACTIVE_SESSIONS_METRIC)
                .tag(FetchSessionCache.VIRTUAL_CLUSTER_TAG, "testVc")
                .tag(FetchSessionCache.ROUTER_TAG, "testRouter")
                .gauge();
        assertThat(sessions).isNotNull();

        var partitions = registry.find(FetchSessionCache.PARTITIONS_CACHED_METRIC)
                .tag(FetchSessionCache.VIRTUAL_CLUSTER_TAG, "testVc")
                .tag(FetchSessionCache.ROUTER_TAG, "testRouter")
                .gauge();
        assertThat(partitions).isNotNull();

        var evictions = registry.find(FetchSessionCache.EVICTIONS_METRIC)
                .tag(FetchSessionCache.VIRTUAL_CLUSTER_TAG, "testVc")
                .tag(FetchSessionCache.ROUTER_TAG, "testRouter")
                .counter();
        assertThat(evictions).isNotNull();
    }

    @Test
    void shouldRemoveMetricsOnClose() {
        var cache = createCache(10, 0);
        cache.close();

        assertThat(registry.find(FetchSessionCache.ACTIVE_SESSIONS_METRIC).gauge()).isNull();
        assertThat(registry.find(FetchSessionCache.PARTITIONS_CACHED_METRIC).gauge()).isNull();
        assertThat(registry.find(FetchSessionCache.EVICTIONS_METRIC).counter()).isNull();
    }

    @Test
    void shouldCreateSessionWhenSlotsAvailable() {
        var cache = createCache(2, 0);
        int id = cache.maybeCreateSession(5, 1000);
        assertThat(id).isGreaterThan(0);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.isValid(id)).isTrue();
    }

    @Test
    void shouldDeclineSessionWhenFullAndNoEvictable() {
        var cache = createCache(1, 10_000);
        int id1 = cache.maybeCreateSession(5, 1000);
        assertThat(id1).isGreaterThan(0);

        // Second session cannot be created — first was created too recently to evict
        int id2 = cache.maybeCreateSession(5, 1001);
        assertThat(id2).isEqualTo(0);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.isValid(id1)).isTrue();
    }

    @Test
    void shouldEvictStaleSessionWhenFull() {
        var cache = createCache(1, 100);
        int id1 = cache.maybeCreateSession(5, 1000);
        assertThat(id1).isGreaterThan(0);

        // Second session created well after minEvictionMs — first is stale
        int id2 = cache.maybeCreateSession(5, 2000);
        assertThat(id2).isGreaterThan(0);
        assertThat(id2).isNotEqualTo(id1);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.isValid(id1)).isFalse();
        assertThat(cache.isValid(id2)).isTrue();
    }

    @Test
    void shouldEvictSmallerSessionWhenFull() {
        var cache = createCache(1, 100);
        int small = cache.maybeCreateSession(2, 1000);
        assertThat(small).isGreaterThan(0);

        // Touch recently so it's not stale, but session has existed > minEvictionMs
        cache.touch(small, 2, 1150);

        // Propose a larger session — should evict the smaller one (not stale, but evictable by size)
        int large = cache.maybeCreateSession(10, 1150);
        assertThat(large).isGreaterThan(0);
        assertThat(cache.isValid(small)).isFalse();
        assertThat(cache.isValid(large)).isTrue();
    }

    @Test
    void shouldNotEvictLargerSessionForSmallerProposal() {
        var cache = createCache(1, 100);
        int large = cache.maybeCreateSession(10, 1000);
        assertThat(large).isGreaterThan(0);

        // Touch recently so it's not stale, but session has existed > minEvictionMs
        cache.touch(large, 10, 1150);

        // Propose a smaller session — should be declined
        int small = cache.maybeCreateSession(2, 1150);
        assertThat(small).isEqualTo(0);
        assertThat(cache.isValid(large)).isTrue();
    }

    @Test
    void shouldPreferStaleEvictionOverSizeEviction() {
        var cache = createCache(1, 100);
        int stale = cache.maybeCreateSession(100, 1000);
        assertThat(stale).isGreaterThan(0);

        // Even though new session is smaller, stale session gets evicted
        int small = cache.maybeCreateSession(1, 2000);
        assertThat(small).isGreaterThan(0);
        assertThat(cache.isValid(stale)).isFalse();
    }

    @Test
    void shouldNotEvictRecentlyCreatedSession() {
        var cache = createCache(1, 10_000);
        int recent = cache.maybeCreateSession(2, 1000);
        assertThat(recent).isGreaterThan(0);

        // New session proposed shortly after — recent session not yet eligible
        int proposed = cache.maybeCreateSession(100, 1500);
        assertThat(proposed).isEqualTo(0);
        assertThat(cache.isValid(recent)).isTrue();
    }

    @Test
    void shouldReleaseSlotOnExplicitRelease() {
        var cache = createCache(1, 10_000);
        int id = cache.maybeCreateSession(5, 1000);
        assertThat(id).isGreaterThan(0);

        cache.release(id);
        assertThat(cache.isValid(id)).isFalse();
        assertThat(cache.size()).isEqualTo(0);

        // Slot is now free
        int id2 = cache.maybeCreateSession(5, 1100);
        assertThat(id2).isGreaterThan(0);
    }

    @Test
    void shouldReportEvictedSessionAsInvalid() {
        var cache = createCache(1, 0);
        int id1 = cache.maybeCreateSession(5, 1000);
        int id2 = cache.maybeCreateSession(5, 2000);
        assertThat(id2).isGreaterThan(0);

        assertThat(cache.isValid(id1)).isFalse();
        assertThat(cache.isValid(id2)).isTrue();
    }

    @Test
    void shouldUpdatePartitionCountOnTouch() {
        var cache = createCache(1, 100);
        int small = cache.maybeCreateSession(2, 1000);
        assertThat(small).isGreaterThan(0);

        // Grow the session's partition count via touch (recently, so not stale)
        cache.touch(small, 100, 1150);

        // A smaller proposal should now be declined (existing is larger and not stale)
        int proposed = cache.maybeCreateSession(5, 1150);
        assertThat(proposed).isEqualTo(0);
    }

    @Test
    void shouldGenerateUniqueSessionIds() {
        var cache = createCache(100, 0);
        var ids = new java.util.HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            int id = cache.maybeCreateSession(1, 1000 + i);
            assertThat(id).isGreaterThan(0);
            assertThat(ids.add(id)).as("Session ID %d should be unique", id).isTrue();
        }
    }

    @Test
    void shouldHandleReleaseOfNonexistentSession() {
        var cache = createCache(10, 0);
        cache.release(999);
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    void shouldHandleTouchOfNonexistentSession() {
        var cache = createCache(10, 0);
        cache.touch(999, 5, 1000);
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    void shouldEvictLeastRecentlyUsedFirst() {
        var cache = createCache(2, 100);
        int id1 = cache.maybeCreateSession(5, 1000);
        int id2 = cache.maybeCreateSession(5, 1001);

        // Touch id1 more recently than id2
        cache.touch(id1, 5, 1500);

        // Third session should evict id2 (least recently used)
        int id3 = cache.maybeCreateSession(5, 2000);
        assertThat(id3).isGreaterThan(0);
        assertThat(cache.isValid(id1)).isTrue();
        assertThat(cache.isValid(id2)).isFalse();
        assertThat(cache.isValid(id3)).isTrue();
    }

    @Test
    void shouldAllowZeroMaxSlots() {
        var cache = createCache(0, 0);
        int id = cache.maybeCreateSession(5, 1000);
        assertThat(id).isEqualTo(0);
        assertThat(cache.size()).isEqualTo(0);
    }
}
