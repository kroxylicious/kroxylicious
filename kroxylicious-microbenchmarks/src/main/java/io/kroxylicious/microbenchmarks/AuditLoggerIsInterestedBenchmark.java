/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.microbenchmarks;

import java.util.List;
import java.util.Map;
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

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;
import io.kroxylicious.proxy.internal.AuditLoggerImpl;

/**
 * Benchmark to measure the overhead of the isInterested() check in AuditEmitter.
 * This benchmark needs sufficient warmup to allow JIT compilation, inlining, and branch prediction
 * to reach steady state.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@State(Scope.Benchmark)
public class AuditLoggerIsInterestedBenchmark {

    @Param({ "0", "1", "3", "5", "10" })
    private int numEmitters;

    @Param({ "ALWAYS_INTERESTED", "NEVER_INTERESTED", "FIRST_INTERESTED", "LAST_INTERESTED", "RANDOM" })
    private EmitterBehavior emitterBehavior;

    private AuditLoggerImpl auditLogger;
    private static final String TEST_ACTION = "TestAction";
    private static final String TEST_STATUS = "success";

    @Setup(Level.Trial)
    public void setup() {
        List<AuditEmitter> emitters = createEmitters(numEmitters, emitterBehavior);
        auditLogger = new AuditLoggerImpl(emitters);
    }

    /**
     * Measures the full audit lifecycle: action() -> withObjectRef() -> log().
     * This includes the isInterested check, builder creation, and emission.
     * When no emitter is interested, the noop builder will be returned and log() does nothing.
     */
    @Benchmark
    public void testFullAuditCycle() {
        auditLogger.action(TEST_ACTION)
                .withObjectRef(Map.of("key", "value"))
                .log();
    }

    private List<AuditEmitter> createEmitters(int count, EmitterBehavior behavior) {
        List<AuditEmitter> emitters = new java.util.ArrayList<>();

        switch (behavior) {
            case ALWAYS_INTERESTED:
                // All emitters return true - branch predictor should optimize this well
                for (int i = 0; i < count; i++) {
                    emitters.add(new TestEmitter(true));
                }
                break;

            case NEVER_INTERESTED:
                // All emitters return false - branch predictor should optimize this well
                // This represents the "audit disabled" or "not interested" case
                for (int i = 0; i < count; i++) {
                    emitters.add(new TestEmitter(false));
                }
                break;

            case FIRST_INTERESTED:
                // First emitter is interested - tests short-circuit optimization
                if (count > 0) {
                    emitters.add(new TestEmitter(true));
                    for (int i = 1; i < count; i++) {
                        emitters.add(new TestEmitter(false));
                    }
                }
                break;

            case LAST_INTERESTED:
                // Last emitter is interested - worst case for short-circuit
                for (int i = 0; i < count - 1; i++) {
                    emitters.add(new TestEmitter(false));
                }
                if (count > 0) {
                    emitters.add(new TestEmitter(true));
                }
                break;

            case RANDOM:
                // Unpredictable pattern - stresses branch predictor
                for (int i = 0; i < count; i++) {
                    emitters.add(new RandomEmitter(i));
                }
                break;
        }

        return emitters;
    }

    public enum EmitterBehavior {
        /** All emitters always interested - predictable true branch */
        ALWAYS_INTERESTED,
        /** All emitters never interested - predictable false branch */
        NEVER_INTERESTED,
        /** First emitter interested - best case for short-circuit */
        FIRST_INTERESTED,
        /** Last emitter interested - worst case for short-circuit */
        LAST_INTERESTED,
        /** Unpredictable pattern - stresses branch predictor */
        RANDOM
    }

    private static class TestEmitter implements AuditEmitter {
        private final boolean interested;

        TestEmitter(boolean interested) {
            this.interested = interested;
        }

        @Override
        public boolean isInterested(String action, String status) {
            return interested;
        }

        @Override
        public void emitAction(AuditableAction action, Context context) {
            // No-op for benchmark
        }

        @Override
        public void close() {
        }
    }

    private static class RandomEmitter implements AuditEmitter {
        private final int seed;

        RandomEmitter(int seed) {
            this.seed = seed;
        }

        @Override
        public boolean isInterested(String action, String status) {
            // Create unpredictable branches based on action hashcode
            // This will defeat branch prediction
            return (action.hashCode() ^ seed) % 3 == 0;
        }

        @Override
        public void emitAction(AuditableAction action, Context context) {
            // No-op for benchmark
        }

        @Override
        public void close() {
        }
    }
}
