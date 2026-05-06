/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.time.Duration;
import java.util.Objects;

import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

public class KroxyliciousUtils {
    private static final ResourceManager resourceManager = ResourceManager.getInstance();

    private KroxyliciousUtils() {
    }

    /**
     * Create certificate config map from listener.
     *
     * @param namespace the namespace
     * @return the tls
     */
    public static Tls createCertificateConfigMapFromListener(String namespace) {
        // wait for listeners to contain data
        var tlsListenerStatus = KafkaUtils.getKafkaListenerStatus("tls");

        var cert = tlsListenerStatus.stream()
                .map(ListenerStatus::getCertificates)
                .findFirst().orElseThrow();

        resourceManager.createResourceFromBuilder(KroxyliciousConfigMapTemplates.getClusterCaConfigMap(namespace, Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT,
                cert.get(0)));
        //@formatter:off
        return new TlsBuilder()
                .withTrustAnchorRef(buildTrustAnchorRef())
                .build();
        //@formatter:on
    }

    @NonNull
    private static TrustAnchorRef buildTrustAnchorRef() {
        // formatter:off
        return new TrustAnchorRefBuilder()
                .withNewRef()
                .withName(Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT)
                .withKind(Constants.CONFIG_MAP)
                .endRef()
                .withKey(Constants.KROXYLICIOUS_TLS_CA_NAME)
                .build();
        // formatter:on
    }

    /**
     * Tls config from cert.
     *
     * @param certNane the cert nane
     * @return the tls
     */
    public static Tls tlsConfigFromCert(String certNane) {
        TlsBuilder tlsBuilder = new TlsBuilder();
        if (certNane != null) {
            // formatter:off
            tlsBuilder
                    .withNewCertificateRef()
                    .withName(certNane)
                    .withKind("Secret")
                    .endCertificateRef();
            // formatter:on
        }
        tlsBuilder.withTrustAnchorRef(buildTrustAnchorRef());
        return tlsBuilder.build();
    }

    /**
     * Waits for KafkaProxy pod to roll out
     * @param namespace the namespace
     * @param previousPodPid the pid of the pod prior to the rollout
     * @param kafkaProxyName the name of the Kafka proxy deployment
     */
    public static void waitForKafkaProxyRolling(String namespace, String previousPodPid, String kafkaProxyName) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(() -> {
            String currentKafkaProxyPod = kubeClient().listPodsByPrefixInName(namespace, kafkaProxyName).get(0).getMetadata().getName();
            return !Objects.equals(DeploymentUtils.getPodUid(namespace, currentKafkaProxyPod), previousPodPid);
        });
    }
}
