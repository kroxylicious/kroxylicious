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
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;
import io.kroxylicious.proxy.internal.AuditLoggerImpl;

/**
 * Investigates whether the JIT can constant-fold and dead-code-eliminate
 * the audit logging path when emitters are known constants.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@State(Scope.Benchmark)
public class AuditLoggerConstantFoldingBenchmark {

    // Static final emitters - JIT should be able to see these are constants
    private static final AuditEmitter NEVER_INTERESTED_EMITTER = new AuditEmitter() {
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

    private static final AuditEmitter ALWAYS_INTERESTED_EMITTER = new AuditEmitter() {
        @Override
        public boolean isInterested(String action, String status) {
            return true;
        }

        @Override
        public void emitAction(AuditableAction action, Context context) {
        }

        @Override
        public void close() {
        }
    };

    // Immutable lists created as constants
    private static final List<AuditEmitter> EMPTY_LIST = List.of();
    private static final List<AuditEmitter> ONE_NEVER = List.of(NEVER_INTERESTED_EMITTER);
    private static final List<AuditEmitter> THREE_NEVER = List.of(
            NEVER_INTERESTED_EMITTER,
            NEVER_INTERESTED_EMITTER,
            NEVER_INTERESTED_EMITTER);

    // Create audit loggers with constant lists
    private static final AuditLoggerImpl EMPTY_LOGGER = new AuditLoggerImpl(EMPTY_LIST);
    private static final AuditLoggerImpl ONE_NEVER_LOGGER = new AuditLoggerImpl(ONE_NEVER);
    private static final AuditLoggerImpl THREE_NEVER_LOGGER = new AuditLoggerImpl(THREE_NEVER);

    private static final String TEST_ACTION = "TestAction";

    /**
     * Baseline: no emitters at all
     */
    @Benchmark
    public void testEmptyList() {
        EMPTY_LOGGER.action(TEST_ACTION)
                .withObjectRef(Map.of("key", "value"))
                .log();
    }

    /**
     * One emitter that's never interested - should JIT optimize this to near-zero?
     */
    @Benchmark
    public void testOneNeverInterested() {
        ONE_NEVER_LOGGER.action(TEST_ACTION)
                .withObjectRef(Map.of("key", "value"))
                .log();
    }

    /**
     * Three emitters never interested - test if JIT can optimize the loop away
     */
    @Benchmark
    public void testThreeNeverInterested() {
        THREE_NEVER_LOGGER.action(TEST_ACTION)
                .withObjectRef(Map.of("key", "value"))
                .log();
    }

    /**
     * Manual inlining to see theoretical best case
     */
    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testManualInline() {
        // Simulate what perfect constant folding would do
        // When isInterested returns false, return noop builder
        noop();
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private void noop() {
        // Intentionally empty
    }
}
