/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Objects;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

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
        return Objects.requireNonNull(kubernetesClientBuilder.build(), "KubernetesClientBuilder.build() returned null");
    }

    public static boolean isKubeClientAvailable() {
        return isKubeClientAvailable(PRESENCE_PROBING_KUBE_CLIENT_BUILD);
    }

    @VisibleForTesting
    static boolean isKubeClientAvailable(KubernetesClientBuilder builder) {
        KubernetesClient client = null;
        try {
            client = builder.build();
            client.namespaces().list();
            return true;
        }
        catch (RuntimeException e) {
            if (client != null) {
                client.close();
            }
            return false;
        }
    }
}
