/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.cmd;

/**
 * A {@link KubeCmdClient} wrapping {@code kubectl}.
 */
public class Kubectl extends BaseCmdKubeClient<Kubectl> {

    public static final String KUBECTL = "kubectl";

    public Kubectl() {
    }

    Kubectl(String futureNamespace) {
        namespace = futureNamespace;
    }

    @Override
    public Kubectl namespace(String namespace) {
        return new Kubectl(namespace);
    }

    @Override
    public String namespace() {
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