/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.util.List;

import io.kroxylicious.proxy.authentication.User;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The means to record an auditable action.
 * This interface is implemented exclusively by the Kroxylicious runtime for consumption by plugins.
 */
public interface AuditLogger<B extends Loggable> {

    /**
     * Start describing a successful auditable action.
     * To actually be recorded {@link AuditableActionBuilder#log()} must be called on the returned builder.
     * @param action The action. Plugins should package-qualify their action names.
     * @return A builder with which to complete the recording of the action.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    B action(String action);

    /**
     * Start describing an unsuccessful auditable action.
     * The failure to permit access to a resource is one example of an unsuccessful auditable action.
     * To actually be recorded {@link AuditableActionBuilder#log()} must be called on the returned builder.
     * @param action The action. Plugins should package-qualify their action names.
     * @param status A machine-readable indication of the reason why the action was unsuccessful.
     * This could be an exception class name or a Kafka error code.
     * @param reason An optional human-readable explanation of why the action was unsuccessful.
     * @return A builder with which to complete the recording of the action.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    B actionWithOutcome(String action, String status, @Nullable String reason);

    <T extends Contextual<T> & Referenceable<T> & Correlatable<T> & Actorable<T> & Loggable> AuditLogger<T> foo();
    <T extends Contextual<T> & Referenceable<T> & Correlatable<T> & Actorable<T> & Loggable,
    U extends Contextual<T> & Referenceable<T> & Correlatable<T> & Loggable> AuditLogger<U> bindActor(AuditLogger<T> logger);

    static void f(AuditLogger<?> l) {
        l.foo().action("Authenticate")
                .addActor(Actorable.PROXY, "2hifv3e4yv93784fgh89")
                .addCoordinate(Referenceable.TOPIC_NAME, "my-topic")
                .addCoordinate(Referenceable.VIRTUAL_CLUSTER, "my-cluster")
                .addCorrelation(Correlatable.CLIENT_CORRELATION_ID, 42)
                .addToContext("action", "f()")
                .addToContext("", "")
                .log();


        l.bindActor(l.foo()).action("Authenticate")
                .addActor(Actorable.CLIENT_PRINCIPLES, List.of(new User("alice")))
                .addActor(Actorable.KAFKA_SESSION_ID, "434tyrbrt")
                .addCoordinate(Referenceable.TOPIC_NAME, "my-topic")
                .addCoordinate(Referenceable.VIRTUAL_CLUSTER, "my-cluster")
                .addCorrelation(Correlatable.CLIENT_CORRELATION_ID, 42)
                .addToContext("action", "f()")
                .addToContext("", "")
                .log();
    }
}
