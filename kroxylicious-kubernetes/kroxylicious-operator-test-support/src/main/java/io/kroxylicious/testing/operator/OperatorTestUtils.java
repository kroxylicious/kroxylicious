/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator;

import java.util.Objects;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utilities for obtaining Kubernetes clients in operator tests and probing whether
 * a Kubernetes cluster is available.
 */
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

    /**
     * Creates a Kubernetes client with default configuration.
     * The caller is responsible for closing the returned client.
     *
     * @return a new Kubernetes client
     */
    public static @NonNull KubernetesClient kubeClient() {
        return kubeClient(new KubernetesClientBuilder());
    }

    /**
     * Creates a Kubernetes client from the given builder.
     * The caller is responsible for closing the returned client.
     *
     * @param kubernetesClientBuilder the builder used to create the client
     * @return a new Kubernetes client
     */
    public static @NonNull KubernetesClient kubeClient(KubernetesClientBuilder kubernetesClientBuilder) {
        return Objects.requireNonNull(kubernetesClientBuilder.build(), "KubernetesClientBuilder.build() returned null");
    }

    /**
     * Determines whether a Kubernetes cluster is reachable, using short timeouts so that
     * the probe fails quickly when no cluster is running (e.g. on a developer's machine).
     *
     * @return {@code true} if a Kubernetes cluster is reachable
     */
    public static boolean isKubeClientAvailable() {
        return isKubeClientAvailable(PRESENCE_PROBING_KUBE_CLIENT_BUILD);
    }

    @VisibleForTesting
    static boolean isKubeClientAvailable(KubernetesClientBuilder builder) {
        try (KubernetesClient client = builder.build()) {
            client.namespaces().list();
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }
}
