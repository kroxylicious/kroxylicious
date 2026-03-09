/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Plugin interface for emitting auditable actions, or data derived from them, outside of this proxy process.
 */
public interface AuditEmitter extends AutoCloseable {

    /**
     * Allows the emitter to short-circuit the creation of an audit event
     * if it has no interest in routing it.
     */
    boolean isInterested(String action, @Nullable String status);

    /**
     * Emit the given action.
     * @param action The action
     * @param context The context
     */
    void emitAction(AuditableAction action, Context context);

    @Override
    void close();

    interface Context {
        String asJsonString(AuditableAction action);
        // byte[] asJsonBytes(AuditableAction action);
    }
}
