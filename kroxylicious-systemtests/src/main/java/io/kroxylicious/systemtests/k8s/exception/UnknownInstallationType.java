/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.exception;

public class UnknownInstallationType extends RuntimeException {
    public UnknownInstallationType(String message) {
        super(message);
    }
}