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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Reconciles a {@link VirtualKafkaCluster} by checking whether the resources
 * referenced by the {@code spec.proxyRef.name}, {@code spec.targetClusterRef.name},
 * {@code spec.ingressRefs[].name} and {@code spec.filterRefs[].name} actually exist,
 * setting a {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.
 */
public final class VirtualKafkaClusterReconciler implements
        Reconciler<VirtualKafkaCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconciler.class);
    public static final String PROXY_EVENT_SOURCE_NAME = "proxy";
    public static final String PROXY_CONFIG_MAP_EVENT_SOURCE_NAME = "proxy-config-map";
    public static final String SERVICES_EVENT_SOURCE_NAME = "services";
    public static final String INGRESSES_EVENT_SOURCE_NAME = "ingresses";
    public static final String FILTERS_EVENT_SOURCE_NAME = "filters";
    public static final String TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND = "TransitivelyReferencedResourcesNotFound";
    public static final String REFERENCED_RESOURCES_NOT_FOUND = "ReferencedResourcesNotFound";

    private final Clock clock;

    public VirtualKafkaClusterReconciler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public UpdateControl<VirtualKafkaCluster> reconcile(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        var existingProxies = context.getSecondaryResource(KafkaProxy.class, PROXY_EVENT_SOURCE_NAME).stream().collect(Collectors.toSet());
        TreeSet<LocalRef<KafkaProxy>> missingProxies = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getProxyRef)
                .stream()
                .collect(Collectors.toCollection(TreeSet::new));
        missingProxies.removeAll(existingProxies.stream()
                .map(ResourcesUtil::toLocalRef)
                .toList());

        var existingServices = context.getSecondaryResource(KafkaService.class, SERVICES_EVENT_SOURCE_NAME).stream().collect(Collectors.toSet());
        TreeSet<LocalRef<KafkaService>> missingServices = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getTargetKafkaServiceRef)
                .stream()
                .collect(Collectors.toCollection(TreeSet::new));
        missingServices.removeAll(existingServices.stream()
                .map(ResourcesUtil::toLocalRef)
                .toList());

        var existingIngresses = context.getSecondaryResources(KafkaProxyIngress.class);
        TreeSet<LocalRef<KafkaProxyIngress>> missingIngresses = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getIngressRefs)
                .stream().flatMap(Collection::stream)
                .collect(Collectors.toCollection(TreeSet::new));
        missingIngresses.removeAll(existingIngresses.stream()
                .map(ResourcesUtil::toLocalRef)
                .toList());

        var existingFilters = context.getSecondaryResources(KafkaProtocolFilter.class);
        TreeSet<LocalRef<KafkaProtocolFilter>> missingFilters = Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getFilterRefs)
                .stream().flatMap(Collection::stream)
                .collect(Collectors.toCollection(TreeSet::new));
        missingFilters.removeAll(existingFilters.stream()
                .map(ResourcesUtil::toLocalRef)
                .toList());

        final List<Condition> conditions;
        if (missingProxies.isEmpty()
                && missingServices.isEmpty()
                && missingIngresses.isEmpty()
                && missingFilters.isEmpty()) {
            var unresolvedServices = existingServices.stream()
                    .filter(ks -> hasAnyResolvedRefsFalse(Optional.ofNullable(ks.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of())))
                    .map(ResourcesUtil::toLocalRef)
                    .collect(Collectors.toCollection(TreeSet::new));
            var unresolvedIngresses = existingIngresses.stream()
                    .filter(kafkaProxyIngress -> hasAnyResolvedRefsFalse(
                            Optional.ofNullable(kafkaProxyIngress.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of())))
                    .map(ResourcesUtil::toLocalRef)
                    .collect(Collectors.toCollection(TreeSet::new));
            var unresolvedFilters = existingFilters.stream()
                    .filter(kpf -> hasAnyResolvedRefsFalse(Optional.ofNullable(kpf.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of())))
                    .map(ResourcesUtil::toLocalRef)
                    .collect(Collectors.toCollection(TreeSet::new));
            if (unresolvedServices.isEmpty()
                    && unresolvedIngresses.isEmpty()
                    && unresolvedFilters.isEmpty()) {
                conditions = context
                        .getSecondaryResource(ConfigMap.class, PROXY_CONFIG_MAP_EVENT_SOURCE_NAME)
                        .flatMap(cm -> Optional.ofNullable(cm.getData()))
                        .map(ProxyConfigData::new)
                        .flatMap(data -> Optional.ofNullable(data.getConditionsForCluster(ResourcesUtil.name(cluster))))
                        .orElse(List.of()); // cm not found, or this cluster missing in the data.
            }
            else {
                Stream<String> serviceMsg = refsMessage("spec.targetKafkaServiceRef references ", cluster, unresolvedServices);
                Stream<String> ingressMsg = refsMessage("spec.ingressRefs references ", cluster, unresolvedIngresses);
                Stream<String> filterMsg = refsMessage("spec.filterRefs references ", cluster, unresolvedFilters);
                conditions = List.of(
                        ResourcesUtil.newResolvedRefsFalse(clock,
                                cluster,
                                TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND,
                                joiningMessages(serviceMsg, ingressMsg, filterMsg)),
                        ResourcesUtil.newConditionBuilder(clock, cluster)
                                .withType(Condition.Type.Accepted).build());
            }
        }
        else {
            Stream<String> proxyMsg = refsMessage("spec.proxyRef references ", cluster, missingProxies);
            Stream<String> serviceMsg = refsMessage("spec.targetKafkaServiceRef references ", cluster, missingServices);
            Stream<String> ingressMsg = refsMessage("spec.ingressRefs references ", cluster, missingIngresses);
            Stream<String> filterMsg = refsMessage("spec.filterRefs references ", cluster, missingFilters);

            conditions = List.of(
                    ResourcesUtil.newResolvedRefsFalse(clock,
                            cluster,
                            REFERENCED_RESOURCES_NOT_FOUND,
                            joiningMessages(proxyMsg, serviceMsg, ingressMsg, filterMsg)),
                    ResourcesUtil.newConditionBuilder(clock, cluster)
                            .withType(Condition.Type.Accepted).build());
        }

        UpdateControl<VirtualKafkaCluster> uc = UpdateControl.patchStatus(ResourcesUtil.patchWithCondition(cluster, conditions));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(cluster), name(cluster));
        }
        return uc;
    }

    @NonNull
    private static String joiningMessages(
                                          Stream<String>... serviceMsg) {
        return Stream.of(serviceMsg).flatMap(Function.identity()).collect(Collectors.joining("; "));
    }

    private static boolean hasAnyResolvedRefsFalse(List<Condition> conditions) {
        return conditions.stream()
                .anyMatch(c -> Condition.Type.ResolvedRefs.equals(c.getType())
                        && Condition.Status.FALSE.equals(c.getStatus()));
    }

    @NonNull
    private static <R extends CustomResource<?, ?>> Stream<String> refsMessage(
                                                                               String prefix,
                                                                               VirtualKafkaCluster cluster,
                                                                               TreeSet<? extends LocalRef<R>> refs) {
        return refs.isEmpty() ? Stream.of()
                : Stream.of(
                        prefix + refs.stream()
                                .map(ref -> ResourcesUtil.namespacedSlug(ref, cluster))
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

        InformerEventSourceConfiguration<ConfigMap> clusterToProxyConfigMap = InformerEventSourceConfiguration.from(
                ConfigMap.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_CONFIG_MAP_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(configMap -> ResourcesUtil.findReferrers(context,
                        configMap,
                        VirtualKafkaCluster.class,
                        cluster -> new AnyLocalRefBuilder().withGroup("").withKind("ConfigMap").withName(cluster.getSpec().getProxyRef().getName()).build()))
                .build();

        InformerEventSourceConfiguration<KafkaService> clusterToService = InformerEventSourceConfiguration.from(
                KafkaService.class,
                VirtualKafkaCluster.class)
                .withName(SERVICES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster,
                        cluster.getSpec().getTargetKafkaServiceRef()))
                .withSecondaryToPrimaryMapper(service -> ResourcesUtil.findReferrers(context,
                        service,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getTargetKafkaServiceRef()))
                .build();

        InformerEventSourceConfiguration<KafkaProxyIngress> clusterToIngresses = InformerEventSourceConfiguration.from(
                KafkaProxyIngress.class,
                VirtualKafkaCluster.class)
                .withName(INGRESSES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getIngressRefs)))
                .withSecondaryToPrimaryMapper(ingress -> ResourcesUtil.findReferrersMulti(context,
                        ingress,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getIngressRefs()))
                .build();

        InformerEventSourceConfiguration<KafkaProtocolFilter> clusterToFilters = InformerEventSourceConfiguration.from(
                KafkaProtocolFilter.class,
                VirtualKafkaCluster.class)
                .withName(FILTERS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getFilterRefs)))
                .withSecondaryToPrimaryMapper(filter -> ResourcesUtil.findReferrersMulti(context,
                        filter,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getFilterRefs()))
                .build();

        return List.of(
                new InformerEventSource<>(clusterToProxy, context),
                new InformerEventSource<>(clusterToProxyConfigMap, context),
                new InformerEventSource<>(clusterToIngresses, context),
                new InformerEventSource<>(clusterToService, context),
                new InformerEventSource<>(clusterToFilters, context));
    }

    @Override
    public ErrorStatusUpdateControl<VirtualKafkaCluster> updateErrorStatus(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        List<Condition> conditions = List.of(
                ResourcesUtil.newUnknownCondition(clock, cluster, Condition.Type.ResolvedRefs, e));
        ErrorStatusUpdateControl<VirtualKafkaCluster> uc = ErrorStatusUpdateControl.patchStatus(ResourcesUtil.patchWithCondition(cluster, conditions));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} for error {}", namespace(cluster), name(cluster), e.toString());
        }
        return uc;
    }
}
