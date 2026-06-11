/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LifecycleExecutorsTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = LifecycleExecutors.newLifecycleExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void runOnLifecycleRunsTaskOnDedicatedThread() {
        // given
        var callerThread = Thread.currentThread();

        // when
        var taskThread = LifecycleExecutors.runOnLifecycle(executor, Thread::currentThread);

        // then — task ran on the lifecycle thread (named prefix), not on the caller's thread.
        // This is the structural enforcement: any work submitted via runOnLifecycle is by
        // construction on the lifecycle executor, not whichever thread happened to call in.
        assertThat(taskThread.getName()).startsWith(LifecycleExecutors.LIFECYCLE_THREAD_NAME_PREFIX);
        assertThat(taskThread).isNotSameAs(callerThread);
    }

    @Test
    void lifecycleThreadIsDaemon() {
        // given — daemonness guarantees the JVM can exit even if the executor is leaked.

        // when
        var isDaemon = LifecycleExecutors.runOnLifecycle(executor, () -> Thread.currentThread().isDaemon());

        // then
        assertThat(isDaemon).isTrue();
    }

    @Test
    void runOnLifecycleSerialisesConcurrentSubmissions() throws Exception {
        // given — two submissions from different caller threads. The lifecycle executor is
        // single-threaded, so the two tasks must observe the same thread inside their body.
        var firstThread = new AtomicReference<Thread>();
        var secondThread = new AtomicReference<Thread>();

        var t1 = new Thread(() -> firstThread.set(LifecycleExecutors.runOnLifecycle(executor, Thread::currentThread)));
        var t2 = new Thread(() -> secondThread.set(LifecycleExecutors.runOnLifecycle(executor, Thread::currentThread)));

        // when
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // then — both pieces of work landed on the same single lifecycle thread.
        assertThat(firstThread.get()).isNotNull();
        assertThat(secondThread.get()).isSameAs(firstThread.get());
    }

    @Test
    void runOnLifecycleUnwrapsRuntimeExceptionFromTask() {
        // given
        var cause = new IllegalStateException("boom");

        // when / then — the original RuntimeException is rethrown unwrapped so callers can
        // catch by type (the by-convention guarantee for FilterFactory.initialize failures).
        assertThatThrownBy(() -> LifecycleExecutors.runOnLifecycle(executor, () -> {
            throw cause;
        })).isSameAs(cause);
    }

    @Test
    void runOnLifecycleWrapsCheckedExceptionFromTask() {
        // given — checked exception thrown by a Callable cannot be rethrown as-is.

        // when / then — wrapped in RuntimeException, but the original cause is preserved.
        var checked = new Exception("checked-boom");
        assertThatThrownBy(() -> LifecycleExecutors.runOnLifecycle(executor, () -> {
            throw checked;
        }))
                .isInstanceOf(RuntimeException.class)
                .hasCause(checked);
    }

    @Test
    void runOnLifecycleReturnsTaskResult() {
        // when
        var result = LifecycleExecutors.runOnLifecycle(executor, () -> "computed-on-lifecycle");

        // then
        assertThat(result).isEqualTo("computed-on-lifecycle");
    }
}
