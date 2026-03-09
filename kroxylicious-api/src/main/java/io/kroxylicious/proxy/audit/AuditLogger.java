/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;

/**
 * The means to record an auditable action.
 * This interface is implemented exclusively by the Kroxylicious runtime for consumption by plugins.
 */
public interface AuditLogger {

    /**
     * Start describing a successful auditable action.
     * To actually be recorded {@link AuditableActionBuilder#log()} must be called on the returned builder.
     * @param action The action. Plugins should package-qualify their action names.
     * @return A builder with which to complete the recording of the action.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder action(String action);

    /**
     * Start describing an unsuccessful auditable action.
     * The failure to permit access to a resource is one example of an unsuccessful auditable action.
     * To actually be recorded {@link AuditableActionBuilder#log()} must be called on the returned builder.
     * @param action The action. Plugins should package-qualify their action names.
     * @return A builder with which to complete the recording of the action.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder actionWithOutcome(String action, String status, String reason);
}
