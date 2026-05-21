/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import java.util.Objects;

/**
 * One per-component failure encountered during a {@code KafkaProxy.reconfigure(Configuration)}
 * call. Contains an identifier suitable for human consumption (logs, alerts, admin endpoints)
 * and the underlying cause for programmatic typed handling.
 *
 * <p><b>Identifier semantics.</b> {@code humanReadableIdentifier} is a best-effort string
 * that identifies which component failed (a virtual cluster name, a referenced filter, etc.).
 * The string's format is implementation-defined and intended for human consumption.
 * Programmatic consumers (triggers deciding whether to roll back, retry, shut down, etc.)
 * should inspect {@link #cause()} for typed failure detection rather than parsing the
 * identifier.
 *
 * @param humanReadableIdentifier a best-effort identifier of the failed component; non-null
 * @param cause                   the underlying failure; non-null
 */
public record ReconfigureError(String humanReadableIdentifier, Throwable cause) {

    public ReconfigureError {
        Objects.requireNonNull(humanReadableIdentifier, "humanReadableIdentifier");
        Objects.requireNonNull(cause, "cause");
    }
}
