/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * Indicates that a Referent has not been reconciled, therefore we are operating on incomplete information.
 * We cannot continue reconciliation as the state of that Referent is undetermined. This should be a transient
 * state. The Referent should be reconciled, and it's updated status event should prompt the Referencing reconciler
 * to reconcile again, this time successfully.
 */
public class StaleReferentStatusException extends RuntimeException {
    public StaleReferentStatusException(String message) {
        super(message);
    }
}
