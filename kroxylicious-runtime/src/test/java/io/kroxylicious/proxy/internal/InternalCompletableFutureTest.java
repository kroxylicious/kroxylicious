/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalCompletableFutureTest {

    private ExecutorService executor;

    @BeforeEach
    void beforeEach() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void afterEach() {
        executor.shutdown();
    }

    @Test
    void asyncChainingMethodExecutesOnThreadOfExecutor() throws Exception {
        var threadOfExecutor = executor.submit(Thread::currentThread).get();
        var future = InternalCompletableFuture.completedFuture(executor, null);

        var actualThread = new AtomicReference<Thread>();
        var result = future.thenAcceptAsync((u) -> {
            assertThat(actualThread).hasValue(null);
            actualThread.set(Thread.currentThread());
        });
        result.join();

        assertThat(result).isCompleted();
        assertThat(actualThread).hasValue(threadOfExecutor);

    }

    @Test
    void minimalStageDisallowsToCompletableFuture() {
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        assertThatThrownBy(stage::toCompletableFuture).isInstanceOf(UnsupportedOperationException.class);
    }

    static Stream<Arguments> chainingMethodsReturnValueDisallowToCompletableFuture() {
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
                Arguments.of(
                        "thenCombineAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombineAsync(other, (u1, u2) -> u1)
                ),
                Arguments.of(
                        "thenCombineAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombineAsync(other, (u1, u2) -> u1, e)
                ),

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

                Arguments.of(
                        "exceptionallyCompose",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyCompose(t -> completed)
                ),
                Arguments.of(
                        "exceptionallyComposeAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyComposeAsync(t -> completed)
                ),
                Arguments.of(
                        "exceptionallyComposeAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyComposeAsync(t -> completed, e)
                ),

                Arguments.of("acceptEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEither(other, u -> {
                })),
                Arguments.of("acceptEitherAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEitherAsync(other, u -> {
                })),
                Arguments.of("acceptEitherAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEitherAsync(other, u -> {
                }, e)),

                Arguments.of("applyToEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEither(other, u -> u)),
                Arguments.of("applyToEitherAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEitherAsync(other, u -> u)),
                Arguments.of(
                        "applyToEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEitherAsync(other, u -> u, e)
                ),

                Arguments.of("runAfterEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEither(other, () -> {
                })),
                Arguments.of("runAfterEitherAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEitherAsync(other, () -> {
                })),
                Arguments.of("runAfterEitherAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEitherAsync(other, () -> {
                }, e)),

                Arguments.of("theAcceptBoth", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBoth(other, (u1, u2) -> {
                })),
                Arguments.of(
                        "thenAcceptBothAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBothAsync(other, (u1, u2) -> {
                        })
                ),
                Arguments.of(
                        "thenAcceptBothAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBothAsync(other, (u1, u2) -> {
                        }, e)
                ),

                Arguments.of("runAfterBoth", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBoth(other, () -> {
                })),
                Arguments.of("runAfterBothAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBothAsync(other, () -> {
                })),
                Arguments.of("runAfterBothAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBothAsync(other, () -> {
                }, e))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void chainingMethodsReturnValueDisallowToCompletableFuture(String name, BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        var result = func.apply(stage, executor);
        assertThatThrownBy(result::toCompletableFuture).isInstanceOf(UnsupportedOperationException.class);
    }
}
