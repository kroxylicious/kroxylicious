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
 * submitted configuration differs from the running configuration in any out-of-scope section.
 *
 * <p>{@code KafkaProxy.reconfigure(Configuration)} applies only the virtual-cluster sections
 * of the configuration and the named filter definitions that those virtual clusters reference.
 * Other configuration sections (management, metrics, admin, etc.) are out of scope: if the
 * submitted configuration differs from the running configuration in any such section, the
 * reconfigure is rejected as a pre-flight check before any virtual-cluster change is attempted.
 * The proxy's running state is unchanged; changes to out-of-scope sections still require a
 * proxy restart.
 *
 * <p>This exception is not thrown synchronously from {@code reconfigure()}; instead the
 * returned future completes exceptionally with this cause.
 */
public class OutOfScopeChangeException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Set<String> differingSections;

    /**
     * @param differingSections the configuration-tree section names that differ between the
     *                          running and submitted configurations; non-null, may be empty
     *                          (though an empty set indicates a programmer error in the proxy
     *                          itself rather than a real out-of-scope change)
     * @throws NullPointerException if {@code differingSections} is {@code null}
     */
    public OutOfScopeChangeException(Set<String> differingSections) {
        super(buildMessage(differingSections));
        this.differingSections = Set.copyOf(differingSections);
    }

    /**
     * Returns the configuration-tree section names that differ between the running and
     * submitted configurations and that the proxy declined to reconcile. The returned set
     * is immutable.
     */
    public Set<String> differingSections() {
        return differingSections;
    }

    private static String buildMessage(Set<String> differingSections) {
        Objects.requireNonNull(differingSections, "differingSections");
        return "reconfigure rejected: out-of-scope sections differ between running and submitted configurations: "
                + differingSections;
    }
}
