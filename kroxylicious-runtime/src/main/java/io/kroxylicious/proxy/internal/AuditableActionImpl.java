/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import io.kroxylicious.proxy.audit.Actor;
import io.kroxylicious.proxy.audit.AuditableAction;
import io.kroxylicious.proxy.audit.Correlation;
import io.kroxylicious.proxy.audit.Value;

import edu.umd.cs.findbugs.annotations.Nullable;

public record AuditableActionImpl(Instant time,
                                  String action,
                                  @Nullable String status,
                                  @Nullable String reason,
                                  Actor actor,
                                  Map<String, String> objectRef,
                                  @Nullable Correlation correlation,
                                  @Nullable Map<String, ? extends Value<?>> context)
        implements AuditableAction {

    public AuditableActionImpl {
        Objects.requireNonNull(time);
        Objects.requireNonNull(action);
        if (reason != null) {
            Objects.requireNonNull(status, "a reason requires a status");
        }
        Objects.requireNonNull(actor);
        Objects.requireNonNull(objectRef);
    }
}
