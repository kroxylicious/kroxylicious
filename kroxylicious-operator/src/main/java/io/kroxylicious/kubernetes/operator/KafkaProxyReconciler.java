/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ContextInitializer;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.ProxyModelBuilder;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

// @formatter:off
@Workflow(dependents = {
        @Dependent(
                name = KafkaProxyReconciler.CONFIG_STATE_DEP,
                type = ProxyConfigStateDependentResource.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.CONFIG_DEP,
                reconcilePrecondition = ProxyConfigReconcilePrecondition.class,
                dependsOn = { KafkaProxyReconciler.CONFIG_STATE_DEP },
                type = ProxyConfigDependentResource.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.DEPLOYMENT_DEP,
                type = ProxyDeploymentDependentResource.class,
                dependsOn = { KafkaProxyReconciler.CONFIG_DEP },
                readyPostcondition = DeploymentReadyCondition.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.CLUSTERS_DEP,
                type = ClusterServiceDependentResource.class,
                dependsOn = { KafkaProxyReconciler.DEPLOYMENT_DEP }
        )
})
// @formatter:on
public class KafkaProxyReconciler implements
        Reconciler<KafkaProxy>,
        ContextInitializer<KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconciler.class);

    public static final String CONFIG_STATE_DEP = "config-state";
    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
    public static final String CLUSTERS_DEP = "clusters";

    private final Clock clock;
    private final SecureConfigInterpolator secureConfigInterpolator;
    private final KafkaProxyStatusFactory statusFactory;

    public KafkaProxyReconciler(Clock clock, SecureConfigInterpolator secureConfigInterpolator) {
        this.statusFactory = new KafkaProxyStatusFactory(Objects.requireNonNull(clock));
        this.clock = clock;
        this.secureConfigInterpolator = secureConfigInterpolator;
    }

    @Override
    public void initContext(
                            KafkaProxy proxy,
                            Context<KafkaProxy> context) {
        ProxyModelBuilder proxyModelBuilder = ProxyModelBuilder.contextBuilder();
        ProxyModel model = proxyModelBuilder.build(proxy, context);
        KafkaProxyContext.init(context, new VirtualKafkaClusterStatusFactory(clock), secureConfigInterpolator, model);
    }

    /**
     * The happy path, where all the dependent resources expressed a desired
     */
    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        var uc = UpdateControl.patchStatus(statusFactory.newTrueConditionStatusPatch(primary, Condition.Type.Ready));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(primary), name(primary));
        }
        return uc;
    }

    /**
     * The unhappy path, where some dependent resource threw an exception
     */
    @Override
    public ErrorStatusUpdateControl<KafkaProxy> updateErrorStatus(KafkaProxy proxy,
                                                                  Context<KafkaProxy> context,
                                                                  Exception e) {
        var uc = ErrorStatusUpdateControl.patchStatus(statusFactory.newUnknownConditionStatusPatch(proxy, Condition.Type.Ready, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(proxy), name(proxy), e.toString());
        }
        return uc;

    }

    @Override
    public List<EventSource<?, KafkaProxy>> prepareEventSources(EventSourceContext<KafkaProxy> context) {
        return List.of(
                eventSourceForFilter(context),
                buildVirtualKafkaClusterInformer(context),
                buildKafkaServiceInformer(context),
                buildKafkaProxyIngressInformer(context));
    }

    private static InformerEventSource<?, KafkaProxy> buildVirtualKafkaClusterInformer(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<VirtualKafkaCluster> configuration = InformerEventSourceConfiguration.from(VirtualKafkaCluster.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(clusterToProxyMapper(context))
                .withPrimaryToSecondaryMapper(proxyToClusterMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<?, KafkaProxy> buildKafkaProxyIngressInformer(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<KafkaProxyIngress> configuration = InformerEventSourceConfiguration.from(KafkaProxyIngress.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(ingressToProxyMapper(context))
                .withPrimaryToSecondaryMapper(proxyToIngressMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<?, KafkaProxy> buildKafkaServiceInformer(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<KafkaService> configuration = InformerEventSourceConfiguration.from(KafkaService.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(kafkaServiceRefToProxyMapper(context))
                .withPrimaryToSecondaryMapper(proxyToKafkaServiceMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaService> kafkaServiceRefToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return kafkaServiceRef -> {
            // find all virtual clusters that reference this kafkaServiceRef

            Set<? extends LocalRef<KafkaProxy>> proxyRefs = ResourcesUtil.resourcesInSameNamespace(context, kafkaServiceRef, VirtualKafkaCluster.class)
                    .filter(vkc -> vkc.getSpec().getTargetKafkaServiceRef().equals(ResourcesUtil.toLocalRef(kafkaServiceRef)))
                    .map(VirtualKafkaCluster::getSpec)
                    .map(VirtualKafkaClusterSpec::getProxyRef)
                    .collect(Collectors.toSet());

            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, kafkaServiceRef, KafkaProxy.class,
                    proxy -> proxyRefs.contains(toLocalRef(proxy)));
            LOGGER.debug("Event source KafkaService SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    /**
     * @param context context
     * @return mapper
     */
    @VisibleForTesting
    static PrimaryToSecondaryMapper<HasMetadata> proxyToKafkaServiceMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            // Load all the virtual clusters for the KafkaProxy, then extract all the referenced KafkaService resource ids.
            Set<? extends LocalRef<KafkaService>> clusterRefs = ResourcesUtil.resourcesInSameNamespace(context, primary, VirtualKafkaCluster.class)
                    .filter(vkc -> {
                        LocalRef<KafkaProxy> proxyRef = vkc.getSpec().getProxyRef();
                        return proxyRef.equals(toLocalRef(primary));
                    })
                    .map(VirtualKafkaCluster::getSpec)
                    .map(VirtualKafkaClusterSpec::getTargetKafkaServiceRef)
                    .collect(Collectors.toSet());

            Set<ResourceID> kafkaServiceRefs = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaService.class,
                    cluster -> clusterRefs.contains(toLocalRef(cluster)));
            LOGGER.debug("Event source KafkaService PrimaryToSecondaryMapper got {}", kafkaServiceRefs);
            return kafkaServiceRefs;
        };
    }

    private static InformerEventSource<KafkaProtocolFilter, KafkaProxy> eventSourceForFilter(EventSourceContext<KafkaProxy> context) {

        var configuration = InformerEventSourceConfiguration.from(KafkaProtocolFilter.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(filterToProxy(context))
                .withPrimaryToSecondaryMapper(proxyToFilters(context))
                .build();

        return new InformerEventSource<>(configuration, context);
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaProxy> proxyToFilters(EventSourceContext<KafkaProxy> context) {
        return (KafkaProxy proxy) -> {
            Set<ResourceID> filterReferences = ResourcesUtil.resourcesInSameNamespace(context, proxy, VirtualKafkaCluster.class)
                    .filter(clusterReferences(proxy))
                    .flatMap(cluster -> Optional.ofNullable(cluster.getSpec().getFilterRefs()).orElse(List.of()).stream())
                    .map(filter -> new ResourceID(filter.getName(), namespace(proxy)))
                    .collect(Collectors.toSet());
            LOGGER.debug("KafkaProxy {} has references to filters {}", ResourceID.fromResource(proxy), filterReferences);
            return filterReferences;
        };
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<HasMetadata> proxyToClusterMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            Set<ResourceID> virtualClustersInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, VirtualKafkaCluster.class,
                    clusterReferences(primary));
            LOGGER.debug("Event source VirtualKafkaCluster PrimaryToSecondaryMapper got {}", virtualClustersInProxyNamespace);
            return virtualClustersInProxyNamespace;
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<VirtualKafkaCluster> clusterToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return cluster -> {
            // we need to reconcile all proxies when a virtual kafka cluster changes in case the proxyRef is updated, we need to update
            // the previously referenced proxy too.
            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, cluster, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source VirtualKafkaCluster SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaProxyIngress> ingressToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return ingress -> {
            // we need to reconcile all proxies when a kafka proxy ingress changes in case the proxyRef is updated, we need to update
            // the previously referenced proxy too.
            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, ingress, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source KafkaProxyIngress SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaProxy> proxyToIngressMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            Set<ResourceID> ingressesInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaProxyIngress.class,
                    ingressReferences(primary));
            LOGGER.debug("Event source KafkaProxyIngress PrimaryToSecondaryMapper got {}", ingressesInProxyNamespace);
            return ingressesInProxyNamespace;
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaProtocolFilter> filterToProxy(EventSourceContext<KafkaProxy> context) {
        return (KafkaProtocolFilter filter) -> {
            // filters don't point to a proxy, but must be in the same namespace as the proxy/proxies which reference the,
            // so when a filter changes we reconcile all the proxies in the same namespace
            Set<ResourceID> proxiesInFilterNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, filter, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source SecondaryToPrimaryMapper got {}", proxiesInFilterNamespace);
            return proxiesInFilterNamespace;
        };
    }

    private static Predicate<VirtualKafkaCluster> clusterReferences(HasMetadata primary) {
        return cluster -> cluster.getSpec().getProxyRef().getName().equals(name(primary));
    }

    private static Predicate<KafkaProxyIngress> ingressReferences(HasMetadata primary) {
        return ingress -> ingress.getSpec().getProxyRef().getName().equals(name(primary));
    }
}
// @formatter:off
