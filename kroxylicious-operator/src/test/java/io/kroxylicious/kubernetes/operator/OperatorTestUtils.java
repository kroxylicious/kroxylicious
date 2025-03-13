/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;

import edu.umd.cs.findbugs.annotations.Nullable;

public class OperatorTestUtils {

    /**
     * The timeouts etc of this client build are tuned to handle the case where Kubernetes isn't present.
     * As might be the case on a developer's machine where minikube isn't running.
     */
    private static final KubernetesClientBuilder PRESENCE_PROBING_KUBE_CLIENT_BUILD = new KubernetesClientBuilder()
            .editOrNewConfig()
            .withRequestRetryBackoffLimit(2)
            .withConnectionTimeout(500)
            .endConfig();

    static @Nullable KubernetesClient kubeClientIfAvailable() {
        return kubeClientIfAvailable(new KubernetesClientBuilder());
    }

    static @Nullable KubernetesClient kubeClientIfAvailable(KubernetesClientBuilder kubernetesClientBuilder) {
        var client = kubernetesClientBuilder.build();
        try {
            client.namespaces().list();
            return client;
        }
        catch (KubernetesClientException e) {
            client.close();
            return null;
        }
    }

    static boolean isKubeClientAvailable() {
        try (var client = kubeClientIfAvailable(PRESENCE_PROBING_KUBE_CLIENT_BUILD)) {
            return client != null;
        }
    }
}
