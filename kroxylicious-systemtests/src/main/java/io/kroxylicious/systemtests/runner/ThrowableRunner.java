/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.runner;

/**
 * The interface Throwable runner.
 */
@FunctionalInterface
public interface ThrowableRunner {
    /**
     * Run.
     *
     * @throws Exception the exception
     */
    void run() throws Exception;
}
