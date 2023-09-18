/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.exception;

import io.kroxylicious.executor.ExecResult;

public class KubeClusterException extends RuntimeException {
    public final ExecResult result;

    public KubeClusterException(ExecResult result, String s) {
        super(s);
        this.result = result;
    }

    public KubeClusterException(Throwable cause) {
        super(cause);
        this.result = null;
    }

    public static class NotFound extends KubeClusterException {

        public NotFound(ExecResult result, String s) {
            super(result, s);
        }
    }

    public static class AlreadyExists extends KubeClusterException {

        public AlreadyExists(ExecResult result, String s) {
            super(result, s);
        }
    }

    public static class InvalidResource extends KubeClusterException {

        public InvalidResource(ExecResult result, String s) {
            super(result, s);
        }
    }
}