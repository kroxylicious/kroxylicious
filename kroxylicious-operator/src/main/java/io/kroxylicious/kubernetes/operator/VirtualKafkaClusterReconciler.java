/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

public final class VirtualKafkaClusterReconciler implements
        Reconciler<VirtualKafkaCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconciler.class);
    public static final String PROXY_EVENT_SOURCE_NAME = "proxy";
    public static final String SERVICES_EVENT_SOURCE_NAME = "services";
    public static final String INGRESSES_EVENT_SOURCE_NAME = "ingresses";
    public static final String FILTERS_EVENT_SOURCE_NAME = "filters";

    private final Clock clock;

    public VirtualKafkaClusterReconciler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public UpdateControl<VirtualKafkaCluster> reconcile(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        var existingProxies = context.getSecondaryResource(KafkaProxy.class, PROXY_EVENT_SOURCE_NAME).stream()
                .map(ResourcesUtil::toLocalRef)
                .collect(Collectors.toSet());
        var missingProxies = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getProxyRef)
                .stream()
                .collect(Collectors.toCollection(TreeSet::new));
        missingProxies.removeAll(existingProxies);

        var existingServices = context.getSecondaryResource(KafkaService.class, SERVICES_EVENT_SOURCE_NAME).stream()
                .map(ResourcesUtil::toLocalRef)
                .collect(Collectors.toSet());
        var missingServices = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getTargetKafkaServiceRef)
                .stream()
                .collect(Collectors.toCollection(TreeSet::new));
        missingServices.removeAll(existingServices);

        var existingIngresses = context.getSecondaryResourcesAsStream(KafkaProxyIngress.class)
                .map(ResourcesUtil::toLocalRef)
                .collect(Collectors.toSet());
        var missingIngresses = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getIngressRefs)
                .stream().flatMap(Collection::stream)
                .collect(Collectors.toCollection(TreeSet::new));
        missingIngresses.removeAll(existingIngresses);

        var existingFilters = context.getSecondaryResourcesAsStream(KafkaProtocolFilter.class)
                .map(ResourcesUtil::toLocalRef)
                .collect(Collectors.toSet());
        var missingFilters = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getFilterRefs)
                .stream().flatMap(Collection::stream)
                .collect(Collectors.toCollection(TreeSet::new));
        missingFilters.removeAll(existingFilters);

        Condition condition;
        if (missingProxies.isEmpty()
                && missingServices.isEmpty()
                && missingIngresses.isEmpty()
                && missingFilters.isEmpty()) {
            condition = ResourcesUtil.newResolvedRefsTrue(clock, cluster);
        }
        else {
            Stream<String> proxyMsg = getMissingProxy("spec.proxyRef references ", KafkaProxy.class, missingProxies);
            Stream<String> serviceMsg = getMissingProxy("spec.targetKafkaServiceRef references ", KafkaService.class, missingServices);
            Stream<String> ingressMsg = getMissingProxy("spec.ingressRefs references ", KafkaProxyIngress.class, missingIngresses);
            Stream<String> filterMsg = getMissingProxy("spec.filterRefs references ", KafkaProtocolFilter.class, missingFilters);

            condition = ResourcesUtil.newResolvedRefsFalse(clock,
                    cluster,
                    "ReferencedResourcesNotFound",
                    Stream.concat(Stream.concat(Stream.concat(proxyMsg, serviceMsg), ingressMsg), filterMsg).collect(Collectors.joining("; ")));
        }

        UpdateControl<VirtualKafkaCluster> uc = UpdateControl.patchStatus(ResourcesUtil.patchWithCondition(cluster, condition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(cluster), name(cluster));
        }
        return uc;
    }

    @NonNull
    private static <R extends CustomResource<?, ?>> Stream<String> getMissingProxy(
                                                  String prefix,
                                                  Class<R> crdClass,
                                                  TreeSet<? extends LocalRef<R>> refs) {
        return refs.isEmpty() ? Stream.of()
                : Stream.of(
                        prefix + refs.stream()
                                .map(ref -> ResourcesUtil.slug(crdClass, ref.getName()))
                                .collect(Collectors.joining(", ")));
    }

    @Override
    public List<EventSource<?, VirtualKafkaCluster>> prepareEventSources(EventSourceContext<VirtualKafkaCluster> context) {
        InformerEventSourceConfiguration<KafkaProxy> clusterToProxy = InformerEventSourceConfiguration.from(
                KafkaProxy.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(proxy -> ResourcesUtil.findReferrers(context,
                        proxy,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getProxyRef()))
                .build();

        InformerEventSourceConfiguration<KafkaService> clusterToService = InformerEventSourceConfiguration.from(
                KafkaService.class,
                VirtualKafkaCluster.class)
                .withName(SERVICES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster,
                        cluster.getSpec().getTargetKafkaServiceRef()))
                .withSecondaryToPrimaryMapper(filter -> ResourcesUtil.findReferrers(context,
                        filter,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getTargetKafkaServiceRef()))
                .build();

        InformerEventSourceConfiguration<KafkaProxyIngress> clusterToIngresses = InformerEventSourceConfiguration.from(
                KafkaProxyIngress.class,
                VirtualKafkaCluster.class)
                .withName(INGRESSES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getIngressRefs)))
                .withSecondaryToPrimaryMapper(filter -> ResourcesUtil.findReferrers2(context,
                        filter,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getIngressRefs()))
                .build();

        InformerEventSourceConfiguration<KafkaProtocolFilter> clusterToFilters = InformerEventSourceConfiguration.from(
                KafkaProtocolFilter.class,
                VirtualKafkaCluster.class)
                .withName(FILTERS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getFilterRefs)))
                .withSecondaryToPrimaryMapper(filter -> ResourcesUtil.findReferrers2(context,
                        filter,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getFilterRefs()))
                .build();

        return List.of(
                new InformerEventSource<>(clusterToProxy, context),
                new InformerEventSource<>(clusterToIngresses, context),
                new InformerEventSource<>(clusterToService, context),
                new InformerEventSource<>(clusterToFilters, context));
    }

    @Override
    public ErrorStatusUpdateControl<VirtualKafkaCluster> updateErrorStatus(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        Condition condition = ResourcesUtil.newUnknownCondition(clock, cluster, Condition.Type.ResolvedRefs, e);
        ErrorStatusUpdateControl<VirtualKafkaCluster> uc = ErrorStatusUpdateControl.patchStatus(ResourcesUtil.patchWithCondition(cluster, condition));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} for error {}", namespace(cluster), name(cluster), e.toString());
        }
        return uc;
    }
}
