/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.audit.AuditLogger;
import io.kroxylicious.proxy.audit.Loggable;

public interface ProxyAuditLogger<B extends Loggable> extends AuditLogger<B>, AutoCloseable {
    @Override
    void close();
}
