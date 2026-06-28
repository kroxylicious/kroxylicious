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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.assertj.core.api.CompletableFutureAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

// we are asserting actual and expected in the correct order, the static actualThread triggers this warning
@SuppressWarnings("java:S3415")
class InternalCompletableFutureTest {

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

    @Test
    void asyncChainingMethodExecutesOnThreadOfExecutor() {
        // Given
        var future = InternalCompletableFuture.completedFuture(executor, null);

        // When
        ThreadCaptor threadCaptor = new ThreadCaptor();
        assertThat(future.thenAcceptAsync(captureThreadConsumer(threadCaptor)))
                .succeedsWithin(Duration.ofMillis(100));

        // Then
        assertThat(threadCaptor.actualThread()).hasValue(threadOfExecutor);
    }

    @Test
    void asyncChainingMethodExecutesOnThreadOfExecutorEither() {
        // Given
        var future = new InternalCompletableFuture<>(executor);

        // When
        ThreadCaptor threadCaptor = new ThreadCaptor();
        assertThat(future.acceptEitherAsync(CompletableFuture.completedFuture(null),
                u -> {
                }).thenAcceptAsync(captureThreadConsumer(threadCaptor)))
                .succeedsWithin(Duration.ofMillis(100));

        // Then
        assertThat(threadCaptor.actualThread()).hasValue(threadOfExecutor);
    }

    record ThreadCaptor(AtomicReference<Thread> actualThread) {
        ThreadCaptor() {
            this(new AtomicReference<>(null));
        }
    }

    record StageChainingContext(CompletionStage<Void> stageToChainTo, Executor executor, ThreadCaptor captor) {}

    static Stream<Arguments> allExceptionalMethods() {
        return Stream.of(
                argumentSet("exceptionally", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().exceptionally(
                        captureThreadThrowableFunction(s.captor()))),
                argumentSet("exceptionallyAsync", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().exceptionallyAsync(
                        captureThreadThrowableFunction(s.captor()))),
                argumentSet("exceptionallyAsync(E)", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().exceptionallyAsync(
                        captureThreadThrowableFunction(s.captor()), s.executor())),

                argumentSet("exceptionallyCompose",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().exceptionallyCompose(
                                captureThreadStageFunction(s.captor()))),
                argumentSet("exceptionallyComposeAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().exceptionallyComposeAsync(
                                captureThreadStageFunction(s.captor()))),
                argumentSet("exceptionallyComposeAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().exceptionallyComposeAsync(
                                captureThreadStageFunction(s.captor()), s.executor())));
    }

    static Stream<Arguments> allCompositionMethods() {
        var other = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                argumentSet("thenCombine", (Function<StageChainingContext, CompletionStage<Void>>) s -> other.thenCombine(s.stageToChainTo(),
                        (unused, unused2) -> null)),
                argumentSet("thenCombineAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.thenCombineAsync(s.stageToChainTo(),
                                (unused, unused2) -> null)),
                argumentSet("thenCombineAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.thenCombineAsync(s.stageToChainTo(),
                                (unused, unused2) -> null, s.executor())),
                argumentSet("acceptEither", (Function<StageChainingContext, CompletionStage<Void>>) s -> other.acceptEither(s.stageToChainTo(),
                        unused -> {
                        })),
                argumentSet("acceptEitherAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.acceptEitherAsync(s.stageToChainTo(),
                                unused -> {
                                })),
                argumentSet("acceptEitherAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.acceptEitherAsync(s.stageToChainTo(),
                                unused -> {
                                }, s.executor())),

                argumentSet("applyToEither", (Function<StageChainingContext, CompletionStage<Void>>) s -> other.applyToEither(s.stageToChainTo(),
                        unused -> null)),
                argumentSet("applyToEitherAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.applyToEitherAsync(s.stageToChainTo(),
                                unused -> null)),
                argumentSet("applyToEitherAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.applyToEitherAsync(s.stageToChainTo(),
                                unused -> null, s.executor())),

                argumentSet("runAfterEither", (Function<StageChainingContext, CompletionStage<Void>>) s -> other.runAfterEither(s.stageToChainTo(),
                        () -> {
                        })),
                argumentSet("runAfterEitherAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.runAfterEitherAsync(s.stageToChainTo(),
                                () -> {
                                })),
                argumentSet("runAfterEitherAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.runAfterEitherAsync(s.stageToChainTo(),
                                () -> {
                                }, s.executor())),

                argumentSet("theAcceptBoth",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.thenAcceptBoth(s.stageToChainTo(),
                                (unused, unused2) -> {
                                })),
                argumentSet("thenAcceptBothAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.thenAcceptBothAsync(s.stageToChainTo(),
                                (unused, unused2) -> {
                                })),
                argumentSet("thenAcceptBothAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.thenAcceptBothAsync(s.stageToChainTo(),
                                (unused, unused2) -> {
                                }, s.executor())),

                argumentSet("runAfterBoth", (Function<StageChainingContext, CompletionStage<Void>>) s -> other.runAfterBoth(s.stageToChainTo(),
                        () -> {
                        })),
                argumentSet("runAfterBothAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.runAfterBothAsync(s.stageToChainTo(),
                                () -> {
                                })),
                argumentSet("runAfterBothAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> other.runAfterBothAsync(s.stageToChainTo(),
                                () -> {
                                }, s.executor())));
    }

    static Stream<Arguments> allChainingMethods() {
        var other = CompletableFuture.<Void> completedFuture(null);
        return Stream.of(
                argumentSet("thenAccept",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenAccept(captureThreadConsumer(s.captor()))),
                argumentSet("thenAcceptAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenAcceptAsync(captureThreadConsumer(s.captor()))),
                argumentSet("thenAcceptAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenAcceptAsync(captureThreadConsumer(s.captor()),
                                s.executor())),

                argumentSet("thenApply", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenApply(
                        captureThreadFunction(s.captor()))),
                argumentSet("thenApplyAsync", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenApplyAsync(
                        captureThreadFunction(s.captor()))),
                argumentSet("thenApplyAsync(E)", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenApplyAsync(
                        captureThreadFunction(s.captor()), s.executor())),

                argumentSet("thenCombine", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenCombine(other,
                        captureThreadComposeBiFunction(s.captor()))),
                argumentSet("thenCombineAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenCombineAsync(other,
                                captureThreadComposeBiFunction(s.captor()))),
                argumentSet("thenCombineAsync(E)",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenCombineAsync(other,
                                captureThreadComposeBiFunction(s.captor()), s.executor())),

                argumentSet("thenCompose", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenCompose(
                        captureThreadStageFunction(s.captor()))),
                argumentSet("thenComposeAsync", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenComposeAsync(
                        captureThreadStageFunction(s.captor()))),
                argumentSet("thenComposeAsync(E)", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenComposeAsync(
                        captureThreadStageFunction(s.captor()), s.executor())),

                argumentSet("thenRun",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenRun(captureThread(s.captor()))),
                argumentSet("thenRunAsync",
                        (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenRunAsync(captureThread(s.captor())),
                        argumentSet("thenRunAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenRunAsync(captureThread(s.captor()), s.executor())),

                        argumentSet("handle",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().handle(
                                        captureThreadComposeBiFunction(s.captor()))),
                        argumentSet("handleAsync", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().handleAsync(
                                captureThreadComposeBiFunction(s.captor()))),
                        argumentSet("handleAsync(E)", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().handleAsync(
                                captureThreadComposeBiFunction(s.captor()), s.executor())),

                        argumentSet("whenComplete",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().whenComplete(captureThreadBiConsumer(s.captor()))),
                        argumentSet("whenCompleteAsync",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().whenCompleteAsync(
                                        captureThreadBiConsumer(s.captor()))),
                        argumentSet("whenCompleteAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().whenCompleteAsync(
                                        captureThreadBiConsumer(s.captor()),
                                        s.executor())),
                        argumentSet("acceptEither", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().acceptEither(other,
                                captureThreadConsumer(s.captor()))),
                        argumentSet("acceptEitherAsync",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().acceptEitherAsync(other,
                                        captureThreadConsumer(s.captor()))),
                        argumentSet("acceptEitherAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().acceptEitherAsync(other,
                                        captureThreadConsumer(s.captor()), s.executor())),

                        argumentSet("applyToEither", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().applyToEither(other,
                                captureThreadFunction(s.captor()))),
                        argumentSet("applyToEitherAsync",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().applyToEitherAsync(other,
                                        captureThreadFunction(s.captor()))),
                        argumentSet("applyToEitherAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().applyToEitherAsync(other,
                                        captureThreadFunction(s.captor()), s.executor())),

                        argumentSet("runAfterEither", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().runAfterEither(other,
                                captureThread(s.captor()))),
                        argumentSet("runAfterEitherAsync",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().runAfterEitherAsync(other,
                                        captureThread(s.captor()))),
                        argumentSet("runAfterEitherAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().runAfterEitherAsync(other,
                                        captureThread(s.captor()), s.executor())),

                        argumentSet("theAcceptBoth",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenAcceptBoth(other,
                                        captureThreadComposeBiConsumer(s.captor()))),
                        argumentSet("thenAcceptBothAsync",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenAcceptBothAsync(other,
                                        captureThreadComposeBiConsumer(s.captor()))),
                        argumentSet("thenAcceptBothAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().thenAcceptBothAsync(other,
                                        captureThreadComposeBiConsumer(s.captor()), s.executor())),

                        argumentSet("runAfterBoth", (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().runAfterBoth(other,
                                captureThread(s.captor()))),
                        argumentSet("runAfterBothAsync",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().runAfterBothAsync(other,
                                        captureThread(s.captor()))),
                        argumentSet("runAfterBothAsync(E)",
                                (Function<StageChainingContext, CompletionStage<Void>>) s -> s.stageToChainTo().runAfterBothAsync(other,
                                        captureThread(s.captor()), s.executor()))));
    }

    @ParameterizedTest()
    @MethodSource("allCompositionMethods")
    void minimalStageCanCompose(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var minimalStage = future.minimalCompletionStage();
        var result = func.apply(new StageChainingContext(minimalStage, executor, new ThreadCaptor()));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest()
    @MethodSource("allCompositionMethods")
    void futureCanCompose(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var result = func.apply(new StageChainingContext(future, executor, new ThreadCaptor()));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoop(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(stage, executor, captor));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource({ "allChainingMethods", "allExceptionalMethods" })
    void chainingToStageProducesFutureOfExpectedType(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();

        // When
        var result = func.apply(new StageChainingContext(stage, executor, new ThreadCaptor()));

        // Then
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
    }

    @ParameterizedTest()
    @MethodSource({ "allChainingMethods", "allExceptionalMethods" })
    void chainingToFutureProducesFutureOfExpectedType(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);

        // When
        var result = func.apply(new StageChainingContext(future, executor, new ThreadCaptor()));

        // Then
        assertThat(result.getClass()).isAssignableTo(InternalCompletableFuture.class);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoop_CompletionDrivenByEventLoop(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(stage, executor, captor));
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.complete(null));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoopFuture(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(future, executor, captor));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.complete(null);

        // Then
        assertThat(resultFuture).isInstanceOf(InternalCompletableFuture.class)
                .succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allChainingMethods")
    void chainedWorkIsExecutedOnEventLoopFuture_CompletionDrivenByEventLoop(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(future, executor, captor));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.complete(null));

        // Then
        assertThat(resultFuture).isInstanceOf(InternalCompletableFuture.class)
                .succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoop(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(stage, executor, captor));
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.completeExceptionally(new IllegalStateException("Whoops it went wrong"));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoop_completionDrivenByEventLoop(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        var stage = future.minimalCompletionStage();
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(stage, executor, captor));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.completeExceptionally(new IllegalStateException("Whoops it went wrong")));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);
        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoopFuture(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(future, executor, captor));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        future.completeExceptionally(new IllegalStateException("Whoops it went wrong"));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);

        // actualThread should be populated by one of the `captureThread` family of methods.
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
    }

    @ParameterizedTest()
    @MethodSource("allExceptionalMethods")
    void exceptionHandlingWorkIsExecutedOnEventLoopFuture_completionDrivenByEventLoop(Function<StageChainingContext, CompletionStage<Void>> func) {
        // Given
        var future = new InternalCompletableFuture<Void>(executor);
        ThreadCaptor captor = new ThreadCaptor();
        var result = func.apply(new StageChainingContext(future, executor, captor));
        CompletableFuture<Void> resultFuture = result.toCompletableFuture();

        // When
        executor.execute(() -> future.completeExceptionally(new IllegalStateException("Whoops it went wrong")));

        // Then
        assertThat(resultFuture).succeedsWithin(2, TimeUnit.SECONDS);

        // actualThread should populated by one of the `captureThread` family of methods.
        assertThat(captor.actualThread()).hasValue(threadOfExecutor);
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
        ThreadCaptor threadCaptor = new ThreadCaptor();
        CompletableFuture<Void> whenCompleteFuture = future.whenComplete(captureThreadBiConsumer(threadCaptor));
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
        assertThat(threadCaptor.actualThread()).hasValue(threadOfExecutor);
    }

    @MethodSource("futureCompletionAlternatives")
    @ParameterizedTest
    void handle(Consumer<CompletableFuture<?>> futureConsumer, boolean ignored) {
        // Given
        CompletableFuture<Void> future = new InternalCompletableFuture<>(executor);
        ThreadCaptor threadCaptor = new ThreadCaptor();
        CompletableFuture<Void> handleFuture = future.handle(captureThreadComposeBiFunction(threadCaptor));

        // When
        futureConsumer.accept(future);

        // Then
        assertThat(handleFuture).succeedsWithin(2, TimeUnit.SECONDS);
        assertThat(threadCaptor.actualThread()).hasValue(threadOfExecutor);
    }

    private static Runnable captureThread(ThreadCaptor threadCaptor) {
        return () -> assertThread(threadCaptor);
    }

    private static <T> Consumer<T> captureThreadConsumer(ThreadCaptor threadCaptor) {
        return ignored -> assertThread(threadCaptor);
    }

    private static <T> BiConsumer<T, Throwable> captureThreadBiConsumer(ThreadCaptor threadCaptor) {
        return (ignored, alsoIgnored) -> assertThread(threadCaptor);
    }

    private static <T> Function<T, T> captureThreadFunction(ThreadCaptor threadCaptor) {
        return value -> {
            assertThread(threadCaptor);
            return value;
        };
    }

    private static <T> Function<Throwable, T> captureThreadThrowableFunction(ThreadCaptor threadCaptor) {
        return ignored -> {
            assertThread(threadCaptor);
            return null;
        };
    }

    private static <T, U> BiFunction<T, U, T> captureThreadComposeBiFunction(ThreadCaptor threadCaptor) {
        return (value, ignored) -> {
            assertThread(threadCaptor);
            return value;
        };
    }

    private static <T, U> BiConsumer<T, U> captureThreadComposeBiConsumer(ThreadCaptor threadCaptor) {
        return (value, ignored) -> {
            assertThread(threadCaptor);
        };
    }

    private static <T, U> Function<T, CompletionStage<U>> captureThreadStageFunction(ThreadCaptor threadCaptor) {
        return value -> {
            assertThread(threadCaptor);
            return CompletableFuture.completedFuture(null);
        };
    }

    private static void assertThread(ThreadCaptor threadCaptor) {
        assertThat(executor.inEventLoop()).isTrue();
        assertThat(threadCaptor.actualThread().compareAndSet(null, Thread.currentThread())).isTrue();
    }

}
