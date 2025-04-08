/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.readiness.Readiness;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

public class OperatorTestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorTestUtils.class);

    /**
     * The timeouts etc of this client build are tuned to handle the case where Kubernetes isn't present.
     * As might be the case on a developer's machine where minikube isn't running.
     */
    private static final KubernetesClientBuilder PRESENCE_PROBING_KUBE_CLIENT_BUILD = new KubernetesClientBuilder()
            .editOrNewConfig()
            .withRequestRetryBackoffLimit(2)
            .withConnectionTimeout(500)
            .endConfig();

    static @NonNull KubernetesClient kubeClient() {
        return kubeClient(new KubernetesClientBuilder());
    }

    static @NonNull KubernetesClient kubeClient(KubernetesClientBuilder kubernetesClientBuilder) {
        KubernetesClient kubernetesClient = kubernetesClientBuilder.build();
        assertThat(kubernetesClient).isNotNull();
        return kubernetesClient;
    }

    static boolean isKubeClientAvailable() {
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

    public static void preloadOperandImage() {
        try (var client = kubeClient()) {
            String operandImage = ProxyDeploymentDependentResource.getOperandImage();
            var pod = client.run().withName("preload-operand-image")
                    .withNewRunConfig()
                    .withImage(operandImage)
                    .withRestartPolicy("Never")
                    .withCommand("ls").done();
            try {
                client.resource(pod).waitUntilCondition(Readiness::isPodSucceeded, 2, TimeUnit.MINUTES);
            }
            finally {
                var reread = client.resource(pod).get();
                if (!Readiness.isPodSucceeded(reread)) {
                    var reasons = reread.getStatus().getContainerStatuses().stream().map(ContainerStatus::getState).map(Objects::toString)
                            .collect(Collectors.joining(","));
                    LOGGER.error("Preloading operand image failed, phase: {}, container state: {}", reread.getStatus().getPhase(), reasons);
                }
                client.resource(pod).delete();
            }
        }
    }
}
