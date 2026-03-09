/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;

import io.kroxylicious.proxy.audit.AuditableActionBuilder;

class NoopAuditableActionBuilder implements AuditableActionBuilder {
    static final NoopAuditableActionBuilder INSTANCE = new NoopAuditableActionBuilder();

    private NoopAuditableActionBuilder() {
    }

    @Override
    public AuditableActionBuilder withObjectRef(Map<String, String> objectRef) {
        return this;
    }

    @Override
    public AuditableActionBuilder withContext(Map<String, String> context) {
        return this;
    }

    @Override
    public AuditableActionBuilder addToContext(String key, String value) {
        return this;
    }

    @Override
    public void log() {
        // do nothing
    }
}
