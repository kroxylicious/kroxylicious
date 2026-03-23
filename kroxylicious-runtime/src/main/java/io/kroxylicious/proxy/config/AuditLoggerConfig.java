/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Objects;

public record AuditLoggerConfig(List<AuditEmitterConfig> emitters) {
    public AuditLoggerConfig {
        Objects.requireNonNull(emitters);
    }
}
