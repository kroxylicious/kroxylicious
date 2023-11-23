/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.exception;

import io.kroxylicious.systemtests.executor.ExecResult;

/**
 * The type Kube cluster exception.
 */
public class KubeClusterException extends RuntimeException {
    /**
     * The Result.
     */
    public final ExecResult result;

    /**
     * Instantiates a new Kube cluster exception.
     *
     * @param result the result
     * @param s the s
     */
    public KubeClusterException(ExecResult result, String s) {
        super(s);
        this.result = result;
    }

    /**
     * Instantiates a new Kube cluster exception.
     *
     * @param cause the cause
     */
    public KubeClusterException(Throwable cause) {
        super(cause);
        this.result = null;
    }

    /**
     * The type Not found.
     */
    public static class NotFound extends KubeClusterException {

        /**
         * Instantiates a new Not found.
         *
         * @param result the result
         * @param s the s
         */
        public NotFound(ExecResult result, String s) {
            super(result, s);
        }
    }

    /**
     * The type Already exists.
     */
    public static class AlreadyExists extends KubeClusterException {

        /**
         * Instantiates a new Already exists.
         *
         * @param result the result
         * @param s the s
         */
        public AlreadyExists(ExecResult result, String s) {
            super(result, s);
        }
    }

    /**
     * The type Invalid resource.
     */
    public static class InvalidResource extends KubeClusterException {

        /**
         * Instantiates a new Invalid resource.
         *
         * @param result the result
         * @param s the s
         */
        public InvalidResource(ExecResult result, String s) {
            super(result, s);
        }
    }
}
