/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

public class OperatorTestUtils {

    private OperatorTestUtils() {
        // Static utils only
    }

    /**
     * The timeouts etc of this client build are tuned to handle the case where Kubernetes isn't present.
     * As might be the case on a developer's machine where minikube isn't running.
     */
    private static final KubernetesClientBuilder PRESENCE_PROBING_KUBE_CLIENT_BUILD = new KubernetesClientBuilder()
            .editOrNewConfig()
            .withRequestRetryBackoffLimit(2)
            .withConnectionTimeout(1000)
            .endConfig();

    public static @NonNull KubernetesClient kubeClient() {
        return kubeClient(new KubernetesClientBuilder());
    }

    public static @NonNull KubernetesClient kubeClient(KubernetesClientBuilder kubernetesClientBuilder) {
        KubernetesClient kubernetesClient = kubernetesClientBuilder.build();
        assertThat(kubernetesClient).isNotNull();
        return kubernetesClient;
    }

    public static boolean isKubeClientAvailable() {
        var client = PRESENCE_PROBING_KUBE_CLIENT_BUILD.build();
        try {
            client.namespaces().list();
            return true;
        }
        catch (KubernetesClientException e) {
            client.close();
            return false;
        }
    }
}
