/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalCompletionStageTest {
    private final CompletableFuture<Void> underlying = new CompletableFuture<>();

    private ThreadPoolExecutor executor;
    private InternalCompletionStage<Void> stage;

    @BeforeEach
    void beforeEach() {
        executor = newSingleThreadThreadPool();
        stage = new InternalCompletionStage<>(underlying, executor);
    }

    @AfterEach
    void afterEach() {
        executor.shutdown();
    }

    static Stream<Arguments> noExecutorFormAsyncMethodsUsesConfiguredExecutor() {
        var other = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                Arguments.of("thenAcceptAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.thenAcceptAsync(u -> noOp())),
                Arguments.of("thenApplyAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.thenApplyAsync(u -> noOp())),
                Arguments.of("thenComposeAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.thenComposeAsync(u -> CompletableFuture.completedStage(noOp()))),
                Arguments.of("thenRunAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.thenRunAsync(() -> noOp())),

                Arguments.of("handleAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.handleAsync((u, t) -> noOp())),
                Arguments.of("whenCompleteAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.whenCompleteAsync((u, t) -> noOp())),

                Arguments.of("exceptionallyAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.exceptionallyAsync((t) -> noOp())),
                Arguments.of("exceptionallyComposeAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s
                                .exceptionallyComposeAsync((t) -> CompletableFuture.completedStage(noOp()))),

                Arguments.of("acceptEitherAsync", (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.acceptEitherAsync(other, (u) -> noOp())),
                Arguments.of("applyToEitherAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.applyToEitherAsync(other, (u) -> noOp())),

                Arguments.of("thenAcceptBothAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.thenAcceptBothAsync(other, (u1, u2) -> noOp())),
                Arguments.of("thenCombineAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.thenCombineAsync(other, (u1, u2) -> noOp())),

                Arguments.of("runAfterBothAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.runAfterBothAsync(other, () -> CompletableFuture.completedStage(
                                noOp()))),
                Arguments.of("runAfterEitherAsync",
                        (Function<CompletionStage<Void>, CompletionStage<Void>>) (s) -> s.runAfterEitherAsync(other, () -> CompletableFuture.completedStage(
                                noOp()))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void noExecutorFormAsyncMethodsUsesConfiguredExecutor(String name, Function<CompletionStage<Void>, CompletionStage<Void>> func) throws Exception {
        var result = func.apply(stage);
        completeUnderlyingFuture(shouldCompleteExceptionally(name));

        assertStageCompletion(result);
        assertThat(executor.getCompletedTaskCount()).isEqualTo(1);
    }

    static Stream<Arguments> executorFormAsyncMethodsUsesCallerExecutor() {
        var other = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                Arguments.of("thenAcceptAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.thenAcceptAsync(u -> noOp(), e)),
                Arguments.of("thenApplyAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.thenApplyAsync(u -> noOp(), e)),
                Arguments.of("thenComposeAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s
                                .thenComposeAsync(u -> CompletableFuture.completedStage(noOp()), e)),
                Arguments.of("thenRunAsync", (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.thenRunAsync(() -> noOp(), e)),

                Arguments.of("handleAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.handleAsync((u, t) -> noOp(), e)),
                Arguments.of("whenCompleteAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.whenCompleteAsync((u, t) -> noOp(), e)),

                Arguments.of("exceptionallyAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.exceptionallyAsync((t) -> noOp(), e)),
                Arguments.of("exceptionallyComposeAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s
                                .exceptionallyComposeAsync((t) -> CompletableFuture.completedStage(noOp()), e)),

                Arguments.of("acceptEitherAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.acceptEitherAsync(other, (u) -> noOp(), e)),
                Arguments.of("applyToEitherAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.applyToEitherAsync(other, (u) -> noOp(), e)),

                Arguments.of("thenAcceptBothAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.thenAcceptBothAsync(other, (u1, u2) -> noOp(), e)),
                Arguments.of("thenCombineAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.thenCombineAsync(other, (u1, u2) -> noOp(), e)),

                Arguments.of("runAfterBothAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.runAfterBothAsync(other, () -> CompletableFuture.completedStage(
                                noOp()), e)),
                Arguments.of("runAfterEitherAsync",
                        (BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>>) (e, s) -> s.runAfterEitherAsync(other,
                                () -> CompletableFuture.completedStage(
                                        noOp()),
                                e)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void executorFormAsyncMethodsUsesCallerExecutor(String name, BiFunction<Executor, CompletionStage<Void>, CompletionStage<Void>> func) throws Exception {

        var callerExecutor = newSingleThreadThreadPool();
        try {
            var result = func.apply(callerExecutor, stage);
            completeUnderlyingFuture(shouldCompleteExceptionally(name));

            assertStageCompletion(result);
            // ensure the action ran on the caller's thread pool, rather than that of the ICS.
            assertThat(executor.getCompletedTaskCount()).isEqualTo(0);
            assertThat(callerExecutor.getCompletedTaskCount()).isEqualTo(1);
        }
        finally {
            callerExecutor.shutdown();
        }
    }

    static Stream<Arguments> chainingMethodsWrapReturnedValue() {
        var other = CompletableFuture.<Void> completedFuture(null);
        var completed = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                Arguments.of("thenAccept", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAccept(u -> {
                })),
                Arguments.of("thenAcceptAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptAsync(u -> {
                })),
                Arguments.of("thenAcceptAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptAsync(u -> {
                }, e)),

                Arguments.of("thenApply", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenApply(u -> u)),
                Arguments.of("thenApplyAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenApplyAsync(u -> u)),
                Arguments.of("thenApplyAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenApplyAsync(u -> u, e)),

                Arguments.of("thenCombine", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombine(other, (u1, u2) -> u1)),
                Arguments.of("thenCombineAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombineAsync(other, (u1, u2) -> u1)),
                Arguments.of("thenCombineAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombineAsync(other, (u1, u2) -> u1, e)),

                Arguments.of("thenCompose", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCompose(u -> completed)),
                Arguments.of("thenComposeAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenComposeAsync(u -> completed)),
                Arguments.of("thenComposeAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenComposeAsync(u -> completed, e)),

                Arguments.of("thenRun", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenRun(() -> {
                })),
                Arguments.of("thenRunAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenRunAsync(() -> {
                })),
                Arguments.of("thenRunAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenRunAsync(() -> {
                }, e)),

                Arguments.of("handle", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.handle((u, t) -> u)),
                Arguments.of("handleAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.handleAsync((u, t) -> u)),
                Arguments.of("handleAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.handleAsync((u, t) -> u, e)),

                Arguments.of("whenComplete", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.whenComplete((u, t) -> {
                })),
                Arguments.of("whenCompleteAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.whenCompleteAsync((u, t) -> {
                })),
                Arguments.of("whenCompleteAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.whenCompleteAsync((u, t) -> {
                }, e)),

                Arguments.of("exceptionally", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionally(t -> null)),
                Arguments.of("exceptionallyAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyAsync(t -> null)),
                Arguments.of("exceptionallyAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyAsync(t -> null, e)),

                Arguments.of("exceptionallyCompose",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyCompose(t -> completed)),
                Arguments.of("exceptionallyComposeAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyComposeAsync(t -> completed)),
                Arguments.of("exceptionallyComposeAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyComposeAsync(t -> completed, e)),

                Arguments.of("acceptEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEither(other, u -> {
                })),
                Arguments.of("acceptEitherAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEitherAsync(other, u -> {
                })),
                Arguments.of("acceptEitherAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEitherAsync(other, u -> {
                }, e)),

                Arguments.of("applyToEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEither(other, u -> u)),
                Arguments.of("applyToEitherAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEitherAsync(other, u -> u)),
                Arguments.of("applyToEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEitherAsync(other, u -> u, e)),

                Arguments.of("runAfterEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEither(other, () -> {
                })),
                Arguments.of("runAfterEitherAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEitherAsync(other, () -> {
                })),
                Arguments.of("runAfterEitherAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEitherAsync(other, () -> {
                }, e)),

                Arguments.of("theAcceptBoth", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBoth(other, (u1, u2) -> {
                })),
                Arguments.of("thenAcceptBothAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBothAsync(other, (u1, u2) -> {
                        })),
                Arguments.of("thenAcceptBothAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBothAsync(other, (u1, u2) -> {
                        }, e)),

                Arguments.of("runAfterBoth", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBoth(other, () -> {
                })),
                Arguments.of("runAfterBothAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBothAsync(other, () -> {
                })),
                Arguments.of("runAfterBothAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBothAsync(other, () -> {
                }, e)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void chainingMethodsWrapReturnedValue(String name, BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) throws Exception {
        var result = func.apply(stage, executor);
        completeUnderlyingFuture(shouldCompleteExceptionally(name));

        assertStageCompletion(result);
        // Verify that the completion stage return is one of ours. The purpose of this is to make sure that thread safety
        // guarantees made about our futures are upheld.
        assertThat((Object) result).isInstanceOf(InternalCompletionStage.class);
    }

    @Test
    void toCompletableFutureDisallowed() {
        assertThatThrownBy(() -> stage.toCompletableFuture()).isInstanceOf(UnsupportedOperationException.class);
    }

    private boolean shouldCompleteExceptionally(String name) {
        return name.startsWith("exceptionally");
    }

    private void completeUnderlyingFuture(boolean completeExceptionally) {
        if (completeExceptionally) {
            underlying.completeExceptionally(new RuntimeException("exceptional completion"));
        }
        else {
            underlying.complete(null);
        }
    }

    private void assertStageCompletion(CompletionStage<Void> result) throws Exception {

        var stageComplete = new CompletableFuture<Void>();
        // use separate executor to avoid the additional action being counted on executor's task count.
        var executor = newSingleThreadThreadPool();
        try {
            var unused = result.whenCompleteAsync((u, t) -> {
                if (t != null) {
                    stageComplete.completeExceptionally(t);
                }
                else {
                    stageComplete.complete(null);
                }
            }, executor);
            stageComplete.get();
        }
        finally {
            executor.shutdown();
        }
    }

    /**
     * This method is invoked by the executor as the chain action to the future.
     * It doesn't need to actually do anything.  The tests make assertions based
     * on the {@link ThreadPoolExecutor#getCompletedTaskCount()} in order to know that
     * the action has been executed by the expected executor.
     * @return null
     */
    private static Void noOp() {
        return null;
    }

    public static ThreadPoolExecutor newSingleThreadThreadPool() {
        int nThreads = 1;
        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
    }
}
