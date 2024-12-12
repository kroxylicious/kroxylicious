
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.cmd;

/**
 * A {@link KubeCmdClient} wrapping {@code kubectl}.
 */
public class Kubectl extends BaseCmdKubeClient<Kubectl> {

    /**
     * The constant KUBECTL.
     */
    public static final String KUBECTL = "kubectl";

    /**
     * Instantiates a new Kubectl.
     */
    public Kubectl() {
    }

    /**
     * Instantiates a new Kubectl.
     *
     * @param futureNamespace the future namespace
     */
    Kubectl(String futureNamespace) {
        namespace = futureNamespace;
    }

    @Override
    public Kubectl getInstanceWithNamespace(String namespace) {
        return new Kubectl(namespace);
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public String defaultNamespace() {
        return "default";
    }

    @Override
    public String cmd() {
        return KUBECTL;
    }
}
