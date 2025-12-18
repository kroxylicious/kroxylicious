/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.assertj.core.api.CompletableFutureAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

// we are asserting actual and expected in the correct order, the static actualThread triggers this warning
@SuppressWarnings("java:S3415")
class InternalCompletableFutureTest {

    private static final AtomicReference<Thread> actualThread = new AtomicReference<>();
    private static EventLoop executor;
    private static Thread threadOfExecutor;

    @BeforeAll
    static void beforeAll() throws Exception {
        executor = new DefaultEventLoop(Executors.newSingleThreadExecutor());
        threadOfExecutor = executor.submit(Thread::currentThread).get();
    }

    @AfterAll
    static void afterAll() {
        executor.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }

    @BeforeEach
    void setUp() {
        actualThread.set(null);
    }

    @Test
    void asyncChainingMethodExecutesOnThreadOfExecutor() {
        // Given
        var future = InternalCompletableFuture.completedFuture(executor, null);

        // When
        assertThat(future.thenAcceptAsync(InternalCompletableFutureTest::captureThread))
                .succeedsWithin(Duration.ofMillis(100));

        // Then
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @Test
    void asyncChainingMethodExecutesOnThreadOfExecutorEither() {
        // Given
        var future = new InternalCompletableFuture<>(executor);

        // When
        assertThat(future.acceptEitherAsync(CompletableFuture.completedFuture(null),
                u -> {
                }).thenAcceptAsync(InternalCompletableFutureTest::captureThread))
                .succeedsWithin(Duration.ofMillis(100));

        // Then
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    static Stream<Arguments> allExceptionalMethods() {
        return Stream.of(
                argumentSet("exceptionally", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionally(
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("exceptionallyAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyAsync(
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("exceptionallyAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyAsync(
                        InternalCompletableFutureTest::captureThreadWithResult, e)),

                argumentSet("exceptionallyCompose",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyCompose(
                                InternalCompletableFutureTest::captureThreadChainedResult)),
                argumentSet("exceptionallyComposeAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyComposeAsync(
                                InternalCompletableFutureTest::captureThreadChainedResult)),
                argumentSet("exceptionallyComposeAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.exceptionallyComposeAsync(
                                InternalCompletableFutureTest::captureThreadChainedResult, e)));
    }

    static Stream<Arguments> allCompositionMethods() {
        var other = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                argumentSet("thenCombine", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.thenCombine(s,
                        (unused, unused2) -> null)),
                argumentSet("thenCombineAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.thenCombineAsync(s,
                                (unused, unused2) -> null)),
                argumentSet("thenCombineAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.thenCombineAsync(s,
                                (unused, unused2) -> null, e)),
                argumentSet("acceptEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.acceptEither(s,
                        unused -> {
                        })),
                argumentSet("acceptEitherAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.acceptEitherAsync(s,
                                unused -> {
                                })),
                argumentSet("acceptEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.acceptEitherAsync(s,
                                unused -> {
                                }, e)),

                argumentSet("applyToEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.applyToEither(s,
                        unused -> null)),
                argumentSet("applyToEitherAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.applyToEitherAsync(s,
                                unused -> null)),
                argumentSet("applyToEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.applyToEitherAsync(s,
                                unused -> null, e)),

                argumentSet("runAfterEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.runAfterEither(s,
                        () -> {
                        })),
                argumentSet("runAfterEitherAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.runAfterEitherAsync(s,
                                () -> {
                                })),
                argumentSet("runAfterEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.runAfterEitherAsync(s,
                                () -> {
                                }, e)),

                argumentSet("theAcceptBoth",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.thenAcceptBoth(s,
                                (unused, unused2) -> {
                                })),
                argumentSet("thenAcceptBothAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.thenAcceptBothAsync(s,
                                (unused, unused2) -> {
                                })),
                argumentSet("thenAcceptBothAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.thenAcceptBothAsync(s,
                                (unused, unused2) -> {
                                }, e)),

                argumentSet("runAfterBoth", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.runAfterBoth(s,
                        () -> {
                        })),
                argumentSet("runAfterBothAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.runAfterBothAsync(s,
                                () -> {
                                })),
                argumentSet("runAfterBothAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> other.runAfterBothAsync(s,
                                () -> {
                                }, e)));
    }

    static Stream<Arguments> allChainingMethods() {
        var other = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                argumentSet("thenAccept",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAccept(InternalCompletableFutureTest::captureThread)),
                argumentSet("thenAcceptAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptAsync(InternalCompletableFutureTest::captureThread)),
                argumentSet("thenAcceptAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptAsync(InternalCompletableFutureTest::captureThread,
                                e)),

                argumentSet("thenApply", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenApply(
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("thenApplyAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenApplyAsync(
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("thenApplyAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenApplyAsync(
                        InternalCompletableFutureTest::captureThreadWithResult, e)),

                argumentSet("thenCombine", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombine(other,
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("thenCombineAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombineAsync(other,
                                InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("thenCombineAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCombineAsync(other,
                                InternalCompletableFutureTest::captureThreadWithResult, e)),

                argumentSet("thenCompose", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenCompose(
                        InternalCompletableFutureTest::captureThreadChainedResult)),
                argumentSet("thenComposeAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenComposeAsync(
                        InternalCompletableFutureTest::captureThreadChainedResult)),
                argumentSet("thenComposeAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenComposeAsync(
                        InternalCompletableFutureTest::captureThreadChainedResult, e)),

                argumentSet("thenRun",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenRun(InternalCompletableFutureTest::captureThread)),
                argumentSet("thenRunAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenRunAsync(InternalCompletableFutureTest::captureThread)),
                argumentSet("thenRunAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenRunAsync(InternalCompletableFutureTest::captureThread, e)),

                argumentSet("handle",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.handle(InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("handleAsync", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.handleAsync(
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("handleAsync(E)", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.handleAsync(
                        InternalCompletableFutureTest::captureThreadWithResult, e)),

                argumentSet("whenComplete",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.whenComplete(InternalCompletableFutureTest::captureThread)),
                argumentSet("whenCompleteAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.whenCompleteAsync(InternalCompletableFutureTest::captureThread)),
                argumentSet("whenCompleteAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.whenCompleteAsync(InternalCompletableFutureTest::captureThread,
                                e)),
                argumentSet("acceptEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEither(other,
                        InternalCompletableFutureTest::captureThread)),
                argumentSet("acceptEitherAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEitherAsync(other,
                                InternalCompletableFutureTest::captureThread)),
                argumentSet("acceptEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.acceptEitherAsync(other,
                                InternalCompletableFutureTest::captureThread, e)),

                argumentSet("applyToEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEither(other,
                        InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("applyToEitherAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEitherAsync(other,
                                InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("applyToEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.applyToEitherAsync(other,
                                InternalCompletableFutureTest::captureThreadWithResult, e)),

                argumentSet("runAfterEither", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEither(other,
                        InternalCompletableFutureTest::captureThread)),
                argumentSet("runAfterEitherAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEitherAsync(other,
                                InternalCompletableFutureTest::captureThread)),
                argumentSet("runAfterEitherAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterEitherAsync(other,
                                InternalCompletableFutureTest::captureThread, e)),

                argumentSet("theAcceptBoth",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBoth(other,
                                InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("thenAcceptBothAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBothAsync(other,
                                InternalCompletableFutureTest::captureThreadWithResult)),
                argumentSet("thenAcceptBothAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.thenAcceptBothAsync(other,
                                InternalCompletableFutureTest::captureThreadWithResult, e)),

                argumentSet("runAfterBoth", (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBoth(other,
                        InternalCompletableFutureTest::captureThread)),
                argumentSet("runAfterBothAsync",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBothAsync(other,
                                InternalCompletableFutureTest::captureThread)),
                argumentSet("runAfterBothAsync(E)",
                        (BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>>) (s, e) -> s.runAfterBothAsync(other,
                                InternalCompletableFutureTest::captureThread, e)));
    }

    @ParameterizedTest()
    @MethodSource("allCompositionMethods")
    void minimalStageCanCompose(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var minimalStage = future.minimalCompletionStage();
        var result = func.apply(minimalStage, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest()
    @MethodSource("allCompositionMethods")
    void futureCanCompose(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var result = func.apply(future, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoop(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        var result = func.apply(stage, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource({ "allChainingMethods", "allExceptionalMethods" })
    void chainingToStageProducesFutureOfExpectedType(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();

        // When
        var result = func.apply(stage, executor);

        // Then
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
    }

    @ParameterizedTest()
    @MethodSource({ "allChainingMethods", "allExceptionalMethods" })
    void chainingToFutureProducesFutureOfExpectedType(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);

        // When
        var result = func.apply(future, executor);

        // Then
        assertThat(result.getClass()).isAssignableTo(InternalCompletableFuture.class);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoop_CompletionDrivenByEventLoop(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        var result = func.apply(stage, executor);
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.complete(null));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoopFuture(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var result = func.apply(future, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).isInstanceOf(InternalCompletableFuture.class)
                .succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoopFuture_CompletionDrivenByEventLoop(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var result = func.apply(future, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.complete(null));

        // Then
        assertThat(resultFuture).isInstanceOf(InternalCompletableFuture.class)
                .succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoop(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        var result = func.apply(stage, executor);
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.completeExceptionally(new IllegalStateException("Whoops it went wrong"));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoop_completionDrivenByEventLoop(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        var result = func.apply(stage, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.completeExceptionally(new IllegalStateException("Whoops it went wrong")));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoopFuture(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var result = func.apply(future, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.completeExceptionally(new IllegalStateException("Whoops it went wrong"));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);

        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoopFuture_completionDrivenByEventLoop(BiFunction<CompletionStage<Void>, Executor, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var result = func.apply(future, executor);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.completeExceptionally(new IllegalStateException("Whoops it went wrong")));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);

        // actualThread should populated by one of the `captureThread` family of methods.
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    static Stream<Arguments> futureCompletionAlternatives() {
        return Stream.of(Arguments.argumentSet("success on eventloop",
                (Consumer<CompletableFuture<?>>) completableFuture -> executor.execute(() -> completableFuture.complete(null)), false),
                Arguments.argumentSet("immediate success",
                        (Consumer<CompletableFuture<?>>) completableFuture -> completableFuture.complete(null), false),
                Arguments.argumentSet("failure on eventloop",
                        (Consumer<CompletableFuture<?>>) completableFuture -> executor.execute(
                                () -> completableFuture.completeExceptionally(new IllegalStateException("Whoops it went wrong"))),
                        true),
                Arguments.argumentSet("immediate failure",
                        (Consumer<CompletableFuture<?>>) completableFuture -> completableFuture.completeExceptionally(new IllegalStateException("Whoops it went wrong")),
                        true));
    }

    @MethodSource("futureCompletionAlternatives")
    @ParameterizedTest
    void whenComplete(Consumer<CompletableFuture<?>> futureConsumer, boolean futureCompletesExceptionally) {
        // Given
        CompletableFuture<Void> future = new InternalCompletableFuture<>(executor);
        CompletableFuture<Void> whenCompleteFuture = future.whenComplete(InternalCompletableFutureTest::captureThread);
        futureConsumer.accept(future);
        CompletableFutureAssert<Void> assertThatWhenComplete = assertThat(whenCompleteFuture);

        // When
        if (futureCompletesExceptionally) {
            assertThatWhenComplete.failsWithin(2, TimeUnit.SECONDS);
        }
        else {
            assertThatWhenComplete.succeedsWithin(2, TimeUnit.SECONDS);
        }

        // Then
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    @MethodSource("futureCompletionAlternatives")
    @ParameterizedTest
    void handle(Consumer<CompletableFuture<?>> futureConsumer, boolean ignored) {
        // Given
        CompletableFuture<Void> future = new InternalCompletableFuture<>(executor);
        CompletableFuture<Void> handleFuture = future.handle(InternalCompletableFutureTest::captureThreadWithResult);

        // When
        futureConsumer.accept(future);

        // Then
        assertThat(handleFuture).succeedsWithin(2, TimeUnit.SECONDS);
        assertThat(actualThread).hasValue(threadOfExecutor);
    }

    private static void captureThread() {
        assertThread();
    }

    private static <T> void captureThread(T ignored) {
        assertThread();
    }

    private static <T> void captureThread(T ignored, Throwable ignoredThrowable) {
        assertThread();
    }

    private static <T> T captureThreadWithResult(T ignored) {
        assertThread();
        return ignored;
    }

    @Nullable
    private static <T> T captureThreadWithResult(Throwable ignored) {
        assertThread();
        return null;
    }

    private static <T, U> T captureThreadWithResult(T ignored, U after) {
        assertThread();
        return ignored;
    }

    private static <T> CompletableFuture<Void> captureThreadChainedResult(T ignored) {
        assertThread();
        return CompletableFuture.completedFuture(null);
    }

    private static void assertThread() {
        assertThat(executor.inEventLoop()).isTrue();
        assertThat(actualThread.compareAndSet(null, Thread.currentThread())).isTrue();
    }

}
