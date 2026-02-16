/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

/**
 * Thrown when a reload operation is attempted while another reload
 * is already in progress. This prevents concurrent configuration changes
 * that could lead to inconsistent state.
 */
public class ConcurrentReloadException extends ReloadException {

    public ConcurrentReloadException(String message) {
        super(message);
    }
}
