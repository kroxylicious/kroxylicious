/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Bounds the number of concurrent client-side fetch sessions across all connections
 * sharing a router definition. Thread-safe; all public methods are synchronized.
 *
 * <p>Eviction follows Kafka's KIP-227 strategy (without the privileged/unprivileged
 * distinction): stale sessions (idle &ge; {@code minEvictionMs}) are evicted first;
 * failing that, the smallest session that has existed long enough is replaced by
 * a larger proposed session. If neither applies the creation is declined and the
 * client operates sessionless.</p>
 */
class FetchSessionCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchSessionCache.class);

    static final int DEFAULT_MAX_SLOTS = 1000;
    static final long DEFAULT_MIN_EVICTION_MS = 120_000L;

    private final int maxSlots;
    private final long minEvictionMs;

    private final Map<Integer, SessionEntry> sessions = new HashMap<>();
    private final TreeMap<LastUsedKey, SessionEntry> lastUsed = new TreeMap<>();
    private final TreeMap<EvictableKey, SessionEntry> evictable = new TreeMap<>();

    FetchSessionCache(int maxSlots,
                      long minEvictionMs) {
        this.maxSlots = maxSlots;
        this.minEvictionMs = minEvictionMs;
    }

    synchronized int maybeCreateSession(int partitionCount,
                                        long nowMs) {
        if (sessions.size() < maxSlots || tryEvict(partitionCount, nowMs)) {
            int id = newSessionId();
            var entry = new SessionEntry(id, nowMs, nowMs, partitionCount);
            sessions.put(id, entry);
            lastUsed.put(entry.lastUsedKey(), entry);
            addToEvictableIfEligible(entry, nowMs);
            LOGGER.atDebug()
                    .addKeyValue("sessionId", id)
                    .addKeyValue("partitionCount", partitionCount)
                    .addKeyValue("activeSlots", sessions.size())
                    .log("Created fetch session");
            return id;
        }
        LOGGER.atDebug()
                .addKeyValue("partitionCount", partitionCount)
                .addKeyValue("activeSlots", sessions.size())
                .log("Declined fetch session creation, cache full");
        return 0;
    }

    synchronized void touch(int sessionId,
                            int partitionCount,
                            long nowMs) {
        var entry = sessions.get(sessionId);
        if (entry == null) {
            return;
        }
        lastUsed.remove(entry.lastUsedKey());
        evictable.remove(entry.evictableKey());

        entry = new SessionEntry(entry.id, entry.creationMs, nowMs, partitionCount);
        sessions.put(sessionId, entry);
        lastUsed.put(entry.lastUsedKey(), entry);
        addToEvictableIfEligible(entry, nowMs);
    }

    synchronized void release(int sessionId) {
        var entry = sessions.remove(sessionId);
        if (entry != null) {
            lastUsed.remove(entry.lastUsedKey());
            evictable.remove(entry.evictableKey());
        }
    }

    synchronized boolean isValid(int sessionId) {
        return sessions.containsKey(sessionId);
    }

    synchronized int size() {
        return sessions.size();
    }

    private boolean tryEvict(int proposedPartitionCount,
                             long nowMs) {
        var stalest = lastUsed.firstEntry();
        if (stalest == null) {
            return false;
        }
        if (nowMs - stalest.getKey().lastUsedMs() >= minEvictionMs) {
            remove(stalest.getValue());
            LOGGER.atTrace()
                    .addKeyValue("sessionId", stalest.getValue().id)
                    .log("Evicted stale fetch session");
            return true;
        }

        var smallest = evictable.firstEntry();
        if (smallest == null) {
            return false;
        }
        var proposedKey = new EvictableKey(proposedPartitionCount, 0);
        if (proposedKey.compareTo(smallest.getKey()) > 0) {
            remove(smallest.getValue());
            LOGGER.atTrace()
                    .addKeyValue("sessionId", smallest.getValue().id)
                    .addKeyValue("evictedPartitions", smallest.getValue().partitionCount)
                    .addKeyValue("proposedPartitions", proposedPartitionCount)
                    .log("Evicted smaller fetch session");
            return true;
        }

        return false;
    }

    private void remove(SessionEntry entry) {
        sessions.remove(entry.id);
        lastUsed.remove(entry.lastUsedKey());
        evictable.remove(entry.evictableKey());
    }

    private void addToEvictableIfEligible(SessionEntry entry,
                                          long nowMs) {
        if (nowMs - entry.creationMs >= minEvictionMs) {
            evictable.put(entry.evictableKey(), entry);
        }
    }

    @SuppressFBWarnings(value = "PREDICTABLE_RANDOM", justification = "Session IDs need uniqueness, not cryptographic strength")
    private int newSessionId() {
        int id;
        do {
            id = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        } while (sessions.containsKey(id));
        return id;
    }

    record LastUsedKey(long lastUsedMs, int id) implements Comparable<LastUsedKey> {
        @Override
        public int compareTo(LastUsedKey other) {
            int cmp = Long.compare(this.lastUsedMs, other.lastUsedMs);
            return cmp != 0 ? cmp : Integer.compare(this.id, other.id);
        }
    }

    record EvictableKey(int size, int id) implements Comparable<EvictableKey> {
        @Override
        public int compareTo(EvictableKey other) {
            int cmp = Integer.compare(this.size, other.size);
            return cmp != 0 ? cmp : Integer.compare(this.id, other.id);
        }
    }

    record SessionEntry(int id, long creationMs, long lastUsedMs, int partitionCount) {
        LastUsedKey lastUsedKey() {
            return new LastUsedKey(lastUsedMs, id);
        }

        EvictableKey evictableKey() {
            return new EvictableKey(partitionCount, id);
        }
    }
}
