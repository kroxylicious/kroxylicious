/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import java.io.Serial;
import java.util.Objects;
import java.util.Set;

/**
 * Exceptional-completion cause for {@code KafkaProxy.reconfigure(Configuration)} when the
 * submitted configuration differs from the running configuration in any <em>static</em>
 * section &mdash; i.e. a section the proxy does not reconcile at runtime.
 *
 * <p>{@code KafkaProxy.reconfigure(Configuration)} reconciles only the virtual-cluster sections
 * of the configuration and the named filter definitions that those virtual clusters reference.
 * Other sections (management, metrics, admin, etc.) are <em>static</em>: their values are fixed
 * at proxy startup and cannot be changed without a proxy restart. If the submitted configuration
 * differs from the running configuration in any such section, the reconfigure is rejected as a
 * pre-flight check before any virtual-cluster change is attempted. The proxy's running state is
 * unchanged.
 *
 * <p><b>Identifier semantics.</b> The {@code humanReadableIdentifiers} returned by
 * {@link #humanReadableIdentifiers()} are best-effort strings that identify the static
 * configuration sections whose values differ between the running and submitted configurations
 * (e.g. {@code "management"}, {@code "metrics"}, {@code "admin"}). The format is
 * implementation-defined and intended for human consumption (logs, alerts, operator-facing
 * surfaces). The same identifier semantics are used by
 * {@link ReconfigureError#humanReadableIdentifier()} for per-component failure reporting;
 * programmatic consumers should treat both as opaque strings to be displayed, not parsed.
 *
 * <p>This exception is not thrown synchronously from {@code reconfigure()}; instead the
 * returned future completes exceptionally with this cause.
 */
public class StaticConfigurationChangedException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Set<String> humanReadableIdentifiers;

    /**
     * @param humanReadableIdentifiers best-effort strings identifying the static configuration
     *                                 sections that differ between the running and submitted
     *                                 configurations; non-null, may be empty (though an empty
     *                                 set indicates a programmer error in the proxy itself
     *                                 rather than a real static-section change)
     * @throws NullPointerException if {@code humanReadableIdentifiers} is {@code null}
     */
    public StaticConfigurationChangedException(Set<String> humanReadableIdentifiers) {
        super(buildMessage(humanReadableIdentifiers));
        this.humanReadableIdentifiers = Set.copyOf(humanReadableIdentifiers);
    }

    /**
     * Returns the best-effort human-readable identifiers of the static configuration sections
     * that differ between the running and submitted configurations and that the proxy declined
     * to reconcile. The returned set is immutable. See the class-level Javadoc for identifier
     * semantics.
     */
    public Set<String> humanReadableIdentifiers() {
        return humanReadableIdentifiers;
    }

    private static String buildMessage(Set<String> humanReadableIdentifiers) {
        Objects.requireNonNull(humanReadableIdentifiers, "humanReadableIdentifiers");
        return "reconfigure rejected: static configuration sections differ between running and submitted configurations: "
                + humanReadableIdentifiers;
    }
}
