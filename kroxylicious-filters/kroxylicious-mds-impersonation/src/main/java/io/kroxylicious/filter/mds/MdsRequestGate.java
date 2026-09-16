/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/** Shared admission control. No waiting queue, sleeping dispatch threads or per-principal state. */
final class MdsRequestGate {
    static final int MAX_IN_FLIGHT = 16;
    private final LongSupplier nanos;
    private final LongSupplier jitter;
    private int inFlight;
    private int failures;
    private long generation;
    private long retryAt;
    private boolean probing;
    private boolean closed;

    record Permit(long generation, boolean probe) {}

    MdsRequestGate() {
        this(System::nanoTime, MdsRequestGate::jitterMillis);
    }

    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Pseudorandomness suffices for retry jitter; not security relevant.
    private static long jitterMillis() {
        return ThreadLocalRandom.current().nextLong(251);
    }

    MdsRequestGate(LongSupplier nanos, LongSupplier jitter) {
        this.nanos = nanos;
        this.jitter = jitter;
    }

    synchronized Permit acquire() {
        if (closed) {
            throw new MdsFailure(MdsFailure.Reason.MDS_CLOSED);
        }
        if (failures > 0 && (probing || inFlight > 0 || nanos.getAsLong() - retryAt < 0)) {
            throw new MdsFailure(MdsFailure.Reason.MDS_BACKOFF);
        }
        if (inFlight == MAX_IN_FLIGHT) {
            throw new MdsFailure(MdsFailure.Reason.MDS_CAPACITY);
        }
        boolean probe = failures > 0;
        probing = probe;
        inFlight++;
        return new Permit(generation, probe);
    }

    synchronized void complete(Permit permit, boolean serviceFailure) {
        inFlight--;
        // An old success must not undo a newer outage. Other old failures must not
        // multiply its backoff; only the single recovery probe advances that state.
        if (permit.generation() != generation) {
            return;
        }
        probing = false;
        if (serviceFailure) {
            failures = Math.min(failures + 1, 5);
            long delayMs = Math.min(500L << (failures - 1), 4750) + jitter.getAsLong();
            retryAt = nanos.getAsLong() + TimeUnit.MILLISECONDS.toNanos(delayMs);
            generation++;
        }
        else if (permit.probe()) {
            failures = 0;
            generation++;
        }
    }

    synchronized void close() {
        closed = true;
    }
}
