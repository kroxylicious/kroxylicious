/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.audit.AuditLogger;
import io.kroxylicious.proxy.audit.AuditableActionBuilder;

import edu.umd.cs.findbugs.annotations.Nullable;

public class NoopAuditLogger implements AuditLogger, AutoCloseable {
    @Override
    public AuditableActionBuilder action(String action) {
        return NoopAuditableActionBuilder.INSTANCE;
    }

    @Override
    public AuditableActionBuilder actionWithOutcome(String action, String status, @Nullable String reason) {
        return NoopAuditableActionBuilder.INSTANCE;
    }

    @Override
    public void close() throws Exception {
        // Nothing to do
    }
}
