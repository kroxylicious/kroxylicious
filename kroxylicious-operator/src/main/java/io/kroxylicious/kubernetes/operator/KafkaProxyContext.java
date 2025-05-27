/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public record KafkaProxyContext(VirtualKafkaClusterStatusFactory virtualKafkaClusterStatusFactory,
                                ProxyModel model,
                                Optional<Configuration> configuration,
                                List<Volume> volumes,
                                List<VolumeMount> mounts) {

    @VisibleForTesting
    static final String KEY_CTX = KafkaProxyContext.class.getName();

    static void init(Context<KafkaProxy> context,
                     VirtualKafkaClusterStatusFactory virtualKafkaClusterStatusFactory,
                     ProxyModel model,
                     @Nullable ConfigurationFragment<Configuration> configuration) {
        var rc = context.managedWorkflowAndDependentResourceContext();

        var fragmentOpt = Optional.ofNullable(configuration);
        Set<Volume> volumes = fragmentOpt.map(ConfigurationFragment::volumes).orElse(Set.of());
        if (volumes.stream().map(Volume::getName).distinct().count() != volumes.size()) {
            throw new IllegalStateException("Two volumes with different definitions share the same name");
        }
        Set<VolumeMount> mounts = fragmentOpt.map(ConfigurationFragment::mounts).orElse(Set.of());
        if (mounts.stream().map(VolumeMount::getMountPath).distinct().count() != mounts.size()) {
            throw new IllegalStateException("Two volume mounts with different definitions share the same mount path");
        }
        rc.put(KEY_CTX,
                new KafkaProxyContext(
                        virtualKafkaClusterStatusFactory,
                        model,
                        fragmentOpt.map(ConfigurationFragment::fragment),
                        volumes.stream().toList(),
                        mounts.stream().toList()));
    }

    static KafkaProxyContext proxyContext(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(KEY_CTX, KafkaProxyContext.class);
    }

    boolean isBroken(VirtualKafkaCluster cluster) {
        var fullyResolved = model().resolutionResult().fullyResolvedClustersInNameOrder().stream().map(ResourcesUtil::name).collect(Collectors.toSet());
        return !fullyResolved.contains(ResourcesUtil.name(cluster))
                || !model().networkingModel().clusterIngressModel(cluster).stream()
                        .allMatch(ingressModel -> ingressModel.ingressExceptions().isEmpty());

    }

}
