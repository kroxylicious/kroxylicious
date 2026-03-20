/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.audit.Actorable;
import io.kroxylicious.proxy.audit.Contextual;
import io.kroxylicious.proxy.audit.Correlatable;
import io.kroxylicious.proxy.audit.Loggable;
import io.kroxylicious.proxy.audit.Referenceable;

class NoopAuditableActionBuilder implements Actorable<NoopAuditableActionBuilder>, Contextual<NoopAuditableActionBuilder>, Correlatable<NoopAuditableActionBuilder>, Referenceable<NoopAuditableActionBuilder>,
        Loggable {
    static final NoopAuditableActionBuilder INSTANCE = new NoopAuditableActionBuilder();

    private NoopAuditableActionBuilder() {
    }

    @Override
    public void log() {
        // do nothing
    }

    @Override
    public NoopAuditableActionBuilder addActor(String scope, String identifier) {
        return this;
    }

    @Override
    public NoopAuditableActionBuilder addToContext(String key, String value) {
        return this;
    }

    @Override
    public NoopAuditableActionBuilder addCorrelation(String scope, String identifier) {
        return this;
    }

    @Override
    public NoopAuditableActionBuilder addCoordinate(String scope, String identifier) {
        return this;
    }
}
