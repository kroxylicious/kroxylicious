/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.util.Map;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;

/**
 * A builder for completing the recording of an auditable action that was started by a call to
 * {@link AuditLogger#action(String)} or {@link AuditLogger#actionWithOutcome(String, String, String)}.
 */
public interface AuditableActionBuilder extends Loggable {

    /**
     * <p>Add the target of the action as a set of coordinates.
     * Each key represents scope (e.g., "vc", "topicId"), and the corresponding value is the unique identifier within that scope.</p>
     *
     * <p>Multiple scopes must be used when:</p>
     * <ul>
     *   <li>a single scope does not provide sufficient uniqueness;
     *     for example an identifier in the "topicName" scope
     *     is only unique within some Kafka cluster, so a scope identifying that
     *     cluster is needed to provide uniqueness.</li>
     *   <li>an identifier is unique but unhelpfully opaque;
     *     for example an identifier in the "topicId" scope is universally unique but does not identify
     *     the containing cluster</li>
     * </ul>
     *
     * <p>Plugins providing their own coordinates must package-prefix their scope names.</p>
     *
     * @param objectRef Coordinates identifying the target object of the action.
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder withObjectRef(Map<String, String> objectRef);
    // TODO the type safety is not perfect because we don't force the caller to call withObjectRef
    // We could do that using an intermediate type for each required thing and put all the
    // optional things on the terminal where log lives.
    // But that makes the API harder to evolve

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, boolean value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, long value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, double value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, String value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, boolean[] value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, long[] value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, double[] value);

    /**
     * <p>Add some additional context to be included with the action.</p>
     * <p>This allows plugins to provide information not known to the runtime.</p>
     * @param key the key
     * @param key the value
     * @return the builder for describing the rest of the action, and ultimately {@linkplain #log()} recording it}.
     */
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    AuditableActionBuilder addToContext(String key, String[] value);

    /**
     * Records the action.
     */
    void log();
}
