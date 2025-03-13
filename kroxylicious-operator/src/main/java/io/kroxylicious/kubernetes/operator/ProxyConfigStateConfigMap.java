/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Comparator;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.ingress.IngressConflictException;
import io.kroxylicious.kubernetes.operator.model.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates a Kube {@code ConfigMap} containing some internal state related to proxy configuration.
 */
@KubernetesDependent
public class ProxyConfigStateConfigMap
        extends CRUDKubernetesDependentResource<ConfigMap, KafkaProxy> {

    public static final String CONFIG_STATE_SUFFIX = "-config-state";

    public ProxyConfigStateConfigMap() {
        super(ConfigMap.class);
    }

    /**
     * @return The {@code metadata.name} of the desired ConfigMap {@code Secret}.
     */
    static String configMapName(KafkaProxy primary) {
        return ResourcesUtil.name(primary) + CONFIG_STATE_SUFFIX;
    }

    @Override
    protected ConfigMap desired(KafkaProxy primary,
                                Context<KafkaProxy> context) {
        var statusFactory = KafkaProxyContext.proxyContext(context).virtualKafkaClusterStatusFactory();
        var data = new ProxyConfigData();

        var proxyModel = KafkaProxyContext.proxyContext(context).model();

        addResolvedRefsConditions(statusFactory, proxyModel, data);
        addAcceptedConditions(statusFactory, proxyModel, data);

        // @formatter:off
        return new ConfigMapBuilder()
                .editOrNewMetadata()
                    .withName(configMapName(primary))
                    .withNamespace(namespace(primary))
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withData(data.build())
                .build();
        // @formatter:on
    }

    private static void addAcceptedConditions(VirtualKafkaClusterStatusFactory statusFactory, ProxyModel proxyModel, ProxyConfigData data) {
        var model = proxyModel.ingressModel();
        for (ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel : model.clusters()) {
            VirtualKafkaCluster cluster = virtualClusterIngressModel.cluster();

            VirtualKafkaCluster patch;
            if (!virtualClusterIngressModel.ingressExceptions().isEmpty()) {
                IngressConflictException first = virtualClusterIngressModel.ingressExceptions().iterator().next();
                patch = statusFactory.newFalseConditionStatusPatch(cluster,
                        Condition.Type.Accepted, Condition.REASON_INVALID,
                        "Ingress(es) [" + first.getIngressName() + "] of cluster conflicts with another ingress");
            }
            else {
                patch = statusFactory.newTrueConditionStatusPatch(cluster,
                        Condition.Type.Accepted);
            }
            if (!data.hasStatusPatchForCluster(ResourcesUtil.name(cluster))) {
                data.addStatusPatchForCluster(ResourcesUtil.name(cluster), patch);
            }
        }
    }

    private static void addResolvedRefsConditions(VirtualKafkaClusterStatusFactory statusFactory, ProxyModel proxyModel, ProxyConfigData data) {
        proxyModel.resolutionResult().clusterResults().stream()
                .filter(ResolutionResult.ClusterResolutionResult::isAnyDependencyUnresolved)
                .forEach(clusterResolutionResult -> {

                    Comparator<LocalRef<?>> comparator = Comparator.<LocalRef<?>, String> comparing(LocalRef::getGroup)
                            .thenComparing(LocalRef::getKind)
                            .thenComparing(LocalRef::getName);

                    LocalRef<?> firstUnresolvedDependency = clusterResolutionResult.unresolvedDependencySet().stream()
                            .sorted(comparator).findFirst()
                            .orElseThrow();
                    String message = String.format("Resource %s was not found.",
                            ResourcesUtil.namespacedSlug(firstUnresolvedDependency, clusterResolutionResult.cluster()));
                    VirtualKafkaCluster cluster = clusterResolutionResult.cluster();

                    data.addStatusPatchForCluster(
                            ResourcesUtil.name(cluster),
                            statusFactory.newFalseConditionStatusPatch(cluster,
                                    Condition.Type.ResolvedRefs, Condition.REASON_INVALID, message));
                });
    }

}
