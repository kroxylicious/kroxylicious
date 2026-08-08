/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

/**
 * Indicates that a declared ingress for a virtual cluster was incompatible with some other ingress of this proxy. For instance,
 * we currently do not support multiple ingresses that need port-per-broker Gateways in the proxy configuration.
 */
public class IngressConflictException extends RuntimeException {

    /** The name of the conflicting ingress resource. */
    private final String ingressName;

    /**
     * Creates a new IngressConflictException.
     * @param ingressName the name of the conflicting ingress
     * @param message the detail message
     */
    public IngressConflictException(String ingressName, String message) {
        super(message);
        this.ingressName = ingressName;
    }

    /**
     * Returns the name of the conflicting ingress.
     * @return the ingress name
     */
    public String getIngressName() {
        return ingressName;
    }
}
