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
    static @Nullable KubernetesClient kubeClientIfAvailable() {
        var client = new KubernetesClientBuilder().build();
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
        try (var client = kubeClientIfAvailable()) {
            return client != null;
        }
    }
}
