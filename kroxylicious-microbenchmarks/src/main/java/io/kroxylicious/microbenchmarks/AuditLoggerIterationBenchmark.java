/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.microbenchmarks;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;

/**
 * Benchmark comparing different iteration strategies for checking emitter interest.
 * Tests: enhanced for-loop with List, indexed loop with List, indexed loop with array.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@State(Scope.Benchmark)
public class AuditLoggerIterationBenchmark {

    @Param({ "1", "3", "5", "10" })
    private int numEmitters;

    private static final String ACTION = "TestAction";
    private static final String STATUS = null;

    // Test with List - enhanced for-loop (current implementation)
    private List<AuditEmitter> listEmitters;

    // Test with List - indexed loop
    private List<AuditEmitter> listEmittersIndexed;

    // Test with array - indexed loop
    private AuditEmitter[] arrayEmitters;

    private static final AuditEmitter NEVER_INTERESTED = new AuditEmitter() {
        @Override
        public boolean isInterested(String action, String status) {
            return false;
        }

        @Override
        public void emitAction(AuditableAction action, Context context) {
        }

        @Override
        public void close() {
        }
    };

    @Setup(Level.Trial)
    public void setup() {
        AuditEmitter[] emitters = new AuditEmitter[numEmitters];
        for (int i = 0; i < numEmitters; i++) {
            emitters[i] = NEVER_INTERESTED;
        }

        // List for enhanced for-loop
        listEmitters = List.of(emitters);

        // List for indexed loop
        listEmittersIndexed = List.of(emitters);

        // Array for indexed loop
        arrayEmitters = emitters;
    }

    /**
     * Current implementation: List with enhanced for-loop
     */
    @Benchmark
    public void testListEnhancedFor(Blackhole bh) {
        boolean interested = anyEmitterHasInterestEnhancedFor(listEmitters);
        bh.consume(interested);
    }

    /**
     * List with indexed for-loop
     */
    @Benchmark
    public void testListIndexedFor(Blackhole bh) {
        boolean interested = anyEmitterHasInterestIndexedList(listEmittersIndexed);
        bh.consume(interested);
    }

    /**
     * Array with indexed for-loop
     */
    @Benchmark
    public void testArrayIndexedFor(Blackhole bh) {
        boolean interested = anyEmitterHasInterestIndexedArray(arrayEmitters);
        bh.consume(interested);
    }

    /**
     * Array with indexed for-loop - no try-catch
     */
    @Benchmark
    public void testArrayIndexedForNoTryCatch(Blackhole bh) {
        boolean interested = anyEmitterHasInterestArrayNoTryCatch(arrayEmitters);
        bh.consume(interested);
    }

    // Simulate current implementation with enhanced for-loop
    private boolean anyEmitterHasInterestEnhancedFor(List<AuditEmitter> emitters) {
        if (emitters != null && !emitters.isEmpty()) {
            for (AuditEmitter emitter : emitters) {
                try {
                    if (emitter.isInterested(ACTION, STATUS)) {
                        return true;
                    }
                }
                catch (Exception e) {
                    // Ignore
                }
            }
        }
        return false;
    }

    // List with indexed loop
    private boolean anyEmitterHasInterestIndexedList(List<AuditEmitter> emitters) {
        if (emitters != null && !emitters.isEmpty()) {
            int size = emitters.size();
            for (int i = 0; i < size; i++) {
                try {
                    if (emitters.get(i).isInterested(ACTION, STATUS)) {
                        return true;
                    }
                }
                catch (Exception e) {
                    // Ignore
                }
            }
        }
        return false;
    }

    // Array with indexed loop
    private boolean anyEmitterHasInterestIndexedArray(AuditEmitter[] emitters) {
        if (emitters != null && emitters.length > 0) {
            for (int i = 0; i < emitters.length; i++) {
                try {
                    if (emitters[i].isInterested(ACTION, STATUS)) {
                        return true;
                    }
                }
                catch (Exception e) {
                    // Ignore
                }
            }
        }
        return false;
    }

    // Array with indexed loop - no try-catch to see overhead
    private boolean anyEmitterHasInterestArrayNoTryCatch(AuditEmitter[] emitters) {
        if (emitters != null && emitters.length > 0) {
            for (int i = 0; i < emitters.length; i++) {
                if (emitters[i].isInterested(ACTION, STATUS)) {
                    return true;
                }
            }
        }
        return false;
    }
}
