/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.function.Supplier;

import io.kroxylicious.proxy.audit.Actor;
import io.kroxylicious.proxy.audit.AuditableActionBuilder;

import edu.umd.cs.findbugs.annotations.Nullable;

public class NoopAuditLogger implements ProxyAuditLogger {
    @Override
    public AuditableActionBuilder action(String action) {
        return NoopAuditableActionBuilder.INSTANCE;
    }

    @Override
    public AuditableActionBuilder actionWithOutcome(String action, String status, @Nullable String reason) {
        return NoopAuditableActionBuilder.INSTANCE;
    }

    @Override
    public void close() {
        // Nothing to do
    }

    @Override
    public ProxyAuditLogger derive(Supplier<Actor> actorSupplier) {
        return this;
    }
}
