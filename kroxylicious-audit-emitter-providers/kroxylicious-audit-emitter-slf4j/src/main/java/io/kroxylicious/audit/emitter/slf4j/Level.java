/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

/**
 * Enumerates the level at which auditable actions will be logged
 */
public enum Level {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    OFF
}
