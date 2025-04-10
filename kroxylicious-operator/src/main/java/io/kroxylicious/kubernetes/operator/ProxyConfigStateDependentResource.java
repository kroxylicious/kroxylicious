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
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates a Kube {@code ConfigMap} containing some internal state related to proxy configuration.
 */
@KubernetesDependent
public class ProxyConfigStateDependentResource
        extends CRUDKubernetesDependentResource<ConfigMap, KafkaProxy> {

    public static final String CONFIG_STATE_CONFIG_MAP_SUFFIX = "-config-state";

    public ProxyConfigStateDependentResource() {
        super(ConfigMap.class);
    }

    /**
     * @return The {@code metadata.name} of the desired ConfigMap {@code Secret}.
     */
    static String configMapName(KafkaProxy primary) {
        return ResourcesUtil.name(primary) + CONFIG_STATE_CONFIG_MAP_SUFFIX;
    }

    @Override
    protected ConfigMap desired(KafkaProxy primary,
                                Context<KafkaProxy> context) {
        KafkaProxyContext proxyContext = KafkaProxyContext.proxyContext(context);
        var statusFactory = proxyContext.virtualKafkaClusterStatusFactory();
        var data = new ProxyConfigStateData();
        var proxyModel = proxyContext.model();
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

    private static void addAcceptedConditions(VirtualKafkaClusterStatusFactory statusFactory, ProxyModel proxyModel, ProxyConfigStateData data) {
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

    private static void addResolvedRefsConditions(VirtualKafkaClusterStatusFactory statusFactory, ProxyModel proxyModel, ProxyConfigStateData data) {
        proxyModel.resolutionResult().clusterResolutionResults().stream()
                .filter(result -> !result.isFullyResolved())
                .forEach(clusterResolutionResult -> {
                    VirtualKafkaCluster cluster = clusterResolutionResult.cluster();
                    VirtualKafkaCluster patch;
                    if (!clusterResolutionResult.danglingReferences().isEmpty()) {
                        Comparator<ClusterResolutionResult.DanglingReference> comparator = Comparator.<ClusterResolutionResult.DanglingReference, LocalRef> comparing(
                                ClusterResolutionResult.DanglingReference::to);

                        LocalRef<?> firstDanglingDependency = clusterResolutionResult.danglingReferences().stream()
                                .sorted(comparator).map(ClusterResolutionResult.DanglingReference::to).findFirst()
                                .orElseThrow();
                        String message = String.format("Resource %s was not found.",
                                ResourcesUtil.namespacedSlug(firstDanglingDependency, cluster));
                        patch = statusFactory.newFalseConditionStatusPatch(cluster,
                                Condition.Type.ResolvedRefs, Condition.REASON_INVALID, message);
                    }
                    else {
                        String message = String.format("Resource %s has ResolvedRefs=False.",
                                ResourcesUtil.namespacedSlug(clusterResolutionResult.findResourcesWithResolvedRefsFalse().sorted().findFirst().orElseThrow(),
                                        cluster));
                        patch = statusFactory.newFalseConditionStatusPatch(cluster,
                                Condition.Type.ResolvedRefs, Condition.REASON_INVALID, message);
                    }
                    data.addStatusPatchForCluster(
                            ResourcesUtil.name(cluster),
                            patch);
                });
    }

}
