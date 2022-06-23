/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.future.FailedFutureException;
import io.kroxylicious.proxy.future.ProxyFuture;
import io.kroxylicious.proxy.future.ProxyPromise;
import io.kroxylicious.proxy.future.UncompletedFutureException;

import static org.junit.jupiter.api.Assertions.*;

class ProxyPromiseImplTest {

    @Test
    public void testNormalNonNullCompletion() {
        normalCompletion(new Object());
    }

    @Test
    public void testNormalNullCompletion() {
        normalCompletion(null);
    }

    private void normalCompletion(Object result) {
        var p = new ProxyPromiseImpl<>();
        assertFalse(p.isDone());
        assertFalse(p.isSuccess());
        assertFalse(p.isFailed());
        assertThrows(UncompletedFutureException.class, p::cause);
        assertThrows(UncompletedFutureException.class, p::value);
        var f = p.future();
        assertFalse(f.isDone());
        assertFalse(f.isSuccess());
        assertFalse(f.isFailed());
        assertThrows(UncompletedFutureException.class, f::cause);
        assertThrows(UncompletedFutureException.class, f::value);

        p.complete(result);

        assertTrue(p.isDone());
        assertTrue(p.isSuccess());
        assertFalse(p.isFailed());
        assertNull(p.cause());
        assertEquals(result, p.value());
        assertTrue(f.isDone());
        assertTrue(f.isSuccess());
        assertFalse(f.isFailed());
        assertNull(f.cause());
        assertEquals(result, f.value());

        assertThrows(IllegalStateException.class, () -> p.complete(new Object()),
                "Completing a completed future should throw");
        assertThrows(IllegalStateException.class, () -> p.fail(new Throwable()),
                "Failing a completed future should throw");

        assertFalse(p.tryComplete(new Object()),
                "tryComplete on a completed future should should return false");
        assertEquals(result, p.value(),
                "Expect the result not to change");

        assertFalse(p.tryFail(new Throwable()),
                "tryFail on a completed future should should return false");
        assertEquals(result, p.value(),
                "Expect the result not to change");
    }

    @Test
    public void testFailureCompletion() {
        Throwable cause = new Throwable();
        var p = new ProxyPromiseImpl<>();
        assertFalse(p.isDone());
        assertFalse(p.isSuccess());
        assertFalse(p.isFailed());
        assertThrows(UncompletedFutureException.class, p::cause);
        assertThrows(UncompletedFutureException.class, p::value);
        var f = p.future();
        assertFalse(f.isDone());
        assertFalse(f.isSuccess());
        assertFalse(f.isFailed());
        assertThrows(UncompletedFutureException.class, f::cause);
        assertThrows(UncompletedFutureException.class, f::value);

        p.fail(cause);

        assertTrue(p.isDone());
        assertFalse(p.isSuccess());
        assertTrue(p.isFailed());
        assertEquals(cause, p.cause());
        assertThrows(FailedFutureException.class, p::value);
        assertTrue(f.isDone());
        assertFalse(f.isSuccess());
        assertTrue(f.isFailed());
        assertEquals(cause, f.cause());
        assertThrows(FailedFutureException.class, f::value);

        assertThrows(IllegalStateException.class, () -> p.complete(new Object()),
                "Completing a completed future should throw");

        assertFalse(p.tryComplete(new Object()),
                "tryComplete on a completed future should should return false");
        assertThrows(FailedFutureException.class, p::value,
                "Expect the result not to change");

        assertFalse(p.tryFail(new Throwable()),
                "tryFail on a failed future should should return false");
        assertEquals(cause, p.cause(),
                "Expect the cause not to change");
    }

    @Test
    public void testCompose() {

        var p = new ProxyPromiseImpl<>();
        Object mappedResult = new Object();
        Object[] observedResult = new Object[]{ null };
        ProxyFuture<Object> composed = p.compose(r -> {
            observedResult[0] = r;
            return ProxyPromise.success(mappedResult);
        });

        Object result = new Object();
        p.complete(result);
        assertEquals(result, observedResult[0]);

        assertTrue(composed.isDone());
        assertTrue(composed.isSuccess());
        assertFalse(composed.isFailed());
        assertNull(composed.cause());
        assertEquals(mappedResult, p.value());

    }

}
