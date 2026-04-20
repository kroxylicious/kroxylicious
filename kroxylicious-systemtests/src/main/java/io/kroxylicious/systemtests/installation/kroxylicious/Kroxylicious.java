/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.kubernetes.api.common.Protocol.TLS;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

public class Kroxylicious {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxylicious.class);
    private KafkaProxy kafkaProxy;
    private KafkaProxyIngress kafkaProxyIngress;
    private List<KafkaProtocolFilter> kafkaProtocolFilters = new ArrayList<>();
    private KafkaService kafkaService;
    private VirtualKafkaCluster virtualKafkaCluster;
    private Tls tls;
    private Tls downstreamTls;
    private final ResourceManager resourceManager = ResourceManager.getInstance();
    private String namespace;

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setTls(Tls tls) {
        this.tls = tls;
    }

    public void setDownstreamTls(Tls downstreamTls) {
        this.downstreamTls = downstreamTls;
    }

    public void setKafkaProxyIngress(KafkaProxyIngress kafkaProxyIngress) {
        if (namespace != null) {
            kafkaProxyIngress = kafkaProxyIngress.edit().editMetadata().withNamespace(namespace).endMetadata().build();
        }
        if (downstreamTls != null) {
            kafkaProxyIngress = kafkaProxyIngress.edit().editSpec().editOrNewClusterIP().withProtocol(TLS).endClusterIP().endSpec().build();
        }
        this.kafkaProxyIngress = kafkaProxyIngress;
    }

    public void setKafkaProxy(KafkaProxy kafkaProxy) {
        if (namespace != null) {
            kafkaProxy = kafkaProxy.edit().editMetadata().withNamespace(namespace).endMetadata().build();
        }
        this.kafkaProxy = kafkaProxy;
    }

    public void setKafkaProtocolFilters(List<KafkaProtocolFilter> kafkaProtocolFilters) {
        if (namespace != null) {
            kafkaProtocolFilters.forEach(kafkaProtocolFilter -> kafkaProtocolFilter.edit().editMetadata().withNamespace(namespace).endMetadata().build());
        }
        this.kafkaProtocolFilters = kafkaProtocolFilters;
    }

    public void setKafkaService(KafkaService kafkaService) {
        if (namespace != null) {
            kafkaService = kafkaService.edit().editMetadata().withNamespace(namespace).endMetadata().build();
        }
        if (tls != null) {
            kafkaService = kafkaService.edit().editSpec()
                    .withBootstrapServers(getKafkaBootstrap("tls", kafkaService.getMetadata().getName()))
                    .withTls(tls)
                    .endSpec()
                    .build();
        }
        this.kafkaService = kafkaService;
    }

    public void setVirtualKafkaCluster(VirtualKafkaCluster virtualKafkaCluster) {
        if (namespace != null) {
            virtualKafkaCluster = virtualKafkaCluster.edit().editMetadata().withNamespace(namespace).endMetadata().build();
        }
        if (downstreamTls != null) {
            virtualKafkaCluster = virtualKafkaCluster.edit()
                    .editSpec()
                    .editIngress(0)
                    .withNewTls()
                    .withCertificateRef(downstreamTls.getCertificateRef())
                    .withTrustAnchorRef(downstreamTls.getTrustAnchorRef())
                    .endTls()
                    .endIngress()
                    .endSpec()
                    .build();
        }
        this.virtualKafkaCluster = virtualKafkaCluster;
    }

    public void createOrUpdateResources() {
        this.kafkaProtocolFilters.forEach(resourceManager::createOrUpdateResourceWithWait);
        resourceManager.createOrUpdateResourceWithWait(this.kafkaProxy, this.kafkaProxyIngress, this.kafkaService, this.virtualKafkaCluster);
    }

    /**
     * Gets bootstrap on the first ingress defined on the virtual cluster.
     *
     * @param namespace the namespace
     * @param clusterName the virtual cluster name
     * @return the bootstrap server of the cluster
     */
    public String getBootstrap(String namespace, String clusterName) {
        String bootstrapServer = await("poll for bootstrap").atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    VirtualKafkaCluster vkc = kubeClient().getClient()
                            .resources(VirtualKafkaCluster.class)
                            .inNamespace(namespace)
                            .withName(clusterName)
                            .get();

                    Optional<Ingresses> first = Optional.ofNullable(vkc)
                            .map(VirtualKafkaCluster::getStatus)
                            .map(VirtualKafkaClusterStatus::getIngresses)
                            .stream()
                            .flatMap(List::stream)
                            .findFirst();
                    return first.map(Ingresses::getBootstrapServer);
                }, Optional::isPresent).orElseThrow();
        LOGGER.debug("Cluster {} bootstrap server: {}", clusterName, bootstrapServer);
        return bootstrapServer;
    }

    /**
     * Gets number of replicas from the underlying deployment.
     *
     * @return the number of replicas
     */
    public int getNumberOfReplicas(String namespace) {
        LOGGER.info("Getting number of replicas..");
        return await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1))
                .until(() -> Optional.ofNullable(kubeClient().getDeployment(namespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME))
                        .map(Deployment::getStatus)
                        .map(DeploymentStatus::getReplicas).orElseThrow(), Objects::nonNull);
    }

    /**
     * Scale replicas to.
     *
     * @param scaledTo the number of replicas to scale up/down
     * @param timeout the timeout
     */
    public void scaleReplicasTo(String namespace, int scaledTo, Duration timeout) {
        LOGGER.info("Scaling number of replicas to {}..", scaledTo);
        kubeClient().getClient().resources(KafkaProxy.class).inNamespace(namespace).withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).scale(scaledTo);
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1))
                .until(() -> getNumberOfReplicas(namespace) == scaledTo && kubeClient().isDeploymentReady(namespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME));
    }

    private static String getKafkaBootstrap(String listenerStatusName, String clusterRefName) {
        // wait for listeners to contain data
        if (KafkaUtils.isKafkaUp(clusterRefName)) {
            var kafkaListenerStatus = KafkaUtils.getKafkaListenerStatus(listenerStatusName);

            return kafkaListenerStatus.stream()
                    .map(ListenerStatus::getBootstrapServers)
                    .findFirst().orElseThrow();
        }
        else {
            // Some operator tests do not need kafka running so we can set a default value
            return String.format("%s-kafka-bootstrap.%s.svc.cluster.local:9092".formatted(clusterRefName, Constants.KAFKA_DEFAULT_NAMESPACE));
        }
    }
}
