/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages the mapping from client-visible producer IDs to per-route producer
 * IDs and epochs, with TTL-based eviction. The TTL resets on every access
 * (put or get), similar to Kafka broker's producer state expiry.
 *
 * <p>Thread-safe: shared across per-connection {@link TopicPartitionRouter}
 * instances for the same virtual cluster.</p>
 */
class ProducerIdManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerIdManager.class);

    static final Duration DEFAULT_TTL = Duration.ofDays(7);
    private static final Duration EVICTION_INTERVAL = Duration.ofMinutes(30);

    record ProducerIdEpoch(long producerId, short producerEpoch) {}

    private record TimestampedMapping(Map<String, ProducerIdEpoch> routeMappings,
                                      long lastAccessNanos) {}

    private final ConcurrentHashMap<Long, TimestampedMapping> mappings = new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final LongSupplier clock;
    @Nullable
    private final ScheduledExecutorService evictor;

    /**
     * Production constructor. Starts a background daemon thread for periodic eviction.
     *
     * @param ttl time after last access before a mapping is eligible for eviction
     */
    ProducerIdManager(Duration ttl) {
        this(ttl, System::nanoTime, createEvictor());
    }

    /**
     * Test constructor. No background evictor — call {@link #evictExpired()} directly.
     *
     * @param ttl time after last access before a mapping is eligible for eviction
     * @param clock supplies the current time in nanoseconds
     */
    @VisibleForTesting
    ProducerIdManager(Duration ttl, LongSupplier clock) {
        this(ttl, clock, null);
    }

    private ProducerIdManager(Duration ttl,
                              LongSupplier clock,
                              @Nullable ScheduledExecutorService evictor) {
        this.ttlNanos = ttl.toNanos();
        this.clock = clock;
        this.evictor = evictor;
        if (evictor != null) {
            long intervalNanos = EVICTION_INTERVAL.toNanos();
            evictor.scheduleWithFixedDelay(
                    this::evictExpired,
                    intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Stores a mapping from a client-visible producer ID to per-route producer IDs.
     *
     * @param clientPid the client-visible producer ID (from the default route's INIT_PRODUCER_ID response)
     * @param routeMappings per-route producer ID and epoch pairs
     */
    void put(long clientPid, Map<String, ProducerIdEpoch> routeMappings) {
        mappings.put(clientPid, new TimestampedMapping(routeMappings, clock.getAsLong()));
    }

    /**
     * Retrieves the per-route mappings for a client-visible producer ID, resetting the TTL.
     *
     * @param clientPid the client-visible producer ID
     * @return the per-route mappings, or null if absent
     */
    @Nullable
    Map<String, ProducerIdEpoch> get(long clientPid) {
        var entry = mappings.computeIfPresent(clientPid,
                (k, existing) -> new TimestampedMapping(existing.routeMappings(), clock.getAsLong()));
        return entry == null ? null : entry.routeMappings();
    }

    /**
     * Removes entries that have not been accessed within the TTL.
     * Called by the background evictor and directly by tests.
     */
    void evictExpired() {
        long now = clock.getAsLong();
        mappings.forEach((pid, entry) -> {
            if (now - entry.lastAccessNanos() > ttlNanos) {
                if (mappings.remove(pid, entry)) {
                    LOGGER.atDebug()
                            .addKeyValue("producerId", pid)
                            .log("Evicted expired producer ID mapping");
                }
            }
        });
    }

    int size() {
        return mappings.size();
    }

    @Override
    public void close() {
        if (evictor != null) {
            evictor.shutdown();
        }
    }

    private static ScheduledExecutorService createEvictor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pid-evictor");
            t.setDaemon(true);
            return t;
        });
    }
}
