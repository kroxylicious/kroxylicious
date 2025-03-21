/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.AggregatedOperatorException;
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
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.generation;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

// @formatter:off
@Workflow(dependents = {
        @Dependent(
                name = KafkaProxyReconciler.CONFIG_DEP,
                type = ProxyConfigConfigMap.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.DEPLOYMENT_DEP,
                type = ProxyDeployment.class,
                dependsOn = { KafkaProxyReconciler.CONFIG_DEP },
                readyPostcondition = DeploymentReadyCondition.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.CLUSTERS_DEP,
                type = ClusterService.class,
                dependsOn = { KafkaProxyReconciler.DEPLOYMENT_DEP }
        )
})
// @formatter:on
public class KafkaProxyReconciler implements
        Reconciler<KafkaProxy>,
        ContextInitializer<KafkaProxy> {


    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconciler.class);

    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
    public static final String CLUSTERS_DEP = "clusters";
    static final String SEC = "sec";
    private final SecureConfigInterpolator secureConfigInterpolator;

    public KafkaProxyReconciler(SecureConfigInterpolator secureConfigInterpolator) {
        this.secureConfigInterpolator = secureConfigInterpolator;
    }

    static SecureConfigInterpolator secureConfigInterpolator(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(SEC, SecureConfigInterpolator.class);
    }

    @Override
    public void initContext(
                            KafkaProxy primary,
                            Context<KafkaProxy> context) {
        context.managedWorkflowAndDependentResourceContext().put(SEC, secureConfigInterpolator);
    }

    /**
     * The happy path, where all the dependent resources expressed a desired
     */
    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        LOGGER.info("Completed reconciliation of {}/{}", namespace(primary), name(primary));
        return UpdateControl.patchStatus(
                buildStatus(primary, context, null));
    }

    /**
     * The unhappy path, where some dependent resource threw an exception
     */
    @Override
    public ErrorStatusUpdateControl<KafkaProxy> updateErrorStatus(KafkaProxy primary,
                                                                  Context<KafkaProxy> context,
                                                                  Exception exception) {
        // Post-condition: status.conditions should be in a canonical order (to avoid non-terminating reconciliations)
        // Post-condition: There is only one Ready condition
        var control = ErrorStatusUpdateControl.patchStatus(buildStatus(primary, context, exception));
        if (exception instanceof InvalidResourceException) {
            control.withNoRetry();
        }
        else {
            control.rescheduleAfter(Duration.ofSeconds(10));
        }
        return control;

    }

    private static KafkaProxy buildStatus(KafkaProxy primary,
                                          Context<KafkaProxy> context,
                                          @Nullable Exception exception) {
        if (exception instanceof AggregatedOperatorException aoe && aoe.getAggregatedExceptions().size() == 1) {
            exception = aoe.getAggregatedExceptions().values().iterator().next();
        }
        var now = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        return new KafkaProxyBuilder(primary)
                .editOrNewStatus()
                    .withObservedGeneration(generation(primary))
                    .withConditions(effectiveReadyCondition(now, primary, exception ))
                    .withClusters(clusterConditions(now, primary, context ))
                .endStatus()
            .build();
        // @formatter:on
    }

    private static List<io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters> clusterConditions(ZonedDateTime now,
                                                                                                             KafkaProxy primary,
                                                                                                             Context<KafkaProxy> context) {
        return ResourcesUtil.clustersInNameOrder(context).map(cluster -> {
            ClusterCondition clusterCondition = SharedKafkaProxyContext.clusterCondition(context, cluster);
            var conditions = newClusterCondition(now, primary, clusterCondition);
            return new io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.ClustersBuilder()
                    .withName(name(cluster))
                    .withConditions(conditions).build();
        }).toList();
    }

    /**
     * Determines whether the {@code Ready} condition has had a state transition,
     * and returns an appropriate new {@code Ready} condition.
     *
     * @param now
     * @param primary The primary.
     * @param exception An exception, or null if the reconciliation was successful.
     * @return The {@code Ready} condition to use in {@code status.conditions}.
     */
    private static Condition effectiveReadyCondition(ZonedDateTime now,
                                                     KafkaProxy primary,
                                                     @Nullable Exception exception) {
        final var oldReady = primary.getStatus() == null || primary.getStatus().getConditions() == null
                ? null
                : primary.getStatus().getConditions().stream().filter(c -> Condition.Type.Ready.equals(c.getType())).findFirst().orElse(null);

        if (isTransition(oldReady, exception)) {
            // reduce verbosity by only logging if we're making a transition
            if (exception != null) {
                logException(primary, exception);
            }
            return newCondition(now, Condition.Type.Ready, primary, exception);
        }
        else {
            oldReady.setObservedGeneration(generation(primary));
            return oldReady;
        }
    }

    static LoggingEventBuilder addResourceKeys(KafkaProxy primary, LoggingEventBuilder loggingEventBuilder) {
        return loggingEventBuilder.addKeyValue("kind", primary.getKind())
                .addKeyValue("group", primary.getGroup())
                .addKeyValue("namespace", namespace(primary))
                .addKeyValue("name", name(primary));
    }

    private static void logException(KafkaProxy primary, Exception exception) {
        if (exception instanceof SchemaValidatedInvalidResourceException) {
            addResourceKeys(primary, LOGGER.atError())
                    .setCause(exception)
                    .log("Operator observed an invalid resource which ought not to have been accepted by the API server. "
                            + "Either the API Server is broken, the CRD is out-of-sync with the operator, or the operator has a bug.");
        }
        else if (exception instanceof InvalidResourceException) {
            addResourceKeys(primary, LOGGER.atWarn())
                    .log("Operator observed an invalid resource: {}", exception.toString());
        }
        else {
            addResourceKeys(primary, LOGGER.atError())
                    .setCause(exception)
                    .log("Operator had unexpected error");
        }
    }

    /**
     * @param now
     * @param primary The primary.
     * @param exception An exception, or null if the reconciliation was successful.
     * @return The {@code Ready} condition to use in {@code status.conditions}
     *         <strong>if the condition had has a state transition</strong>.
     */
    private static Condition newCondition(
                                          ZonedDateTime now,
                                          Condition.Type conditionType,
                                          KafkaProxy primary,
                                          @Nullable Exception exception) {
        return new ConditionBuilder()
                .withLastTransitionTime(now)
                .withMessage(conditionMessage(exception))
                .withObservedGeneration(generation(primary))
                .withReason(conditionReason(exception))
                .withStatus(exception == null ? Condition.Status.TRUE : Condition.Status.FALSE)
                .withType(conditionType)
                .build();
    }

    private static Condition newClusterCondition(
                                                 ZonedDateTime now,
                                                 KafkaProxy primary,
                                                 ClusterCondition clusterCondition) {
        return new ConditionBuilder()
                .withLastTransitionTime(now)
                .withMessage(clusterCondition.message())
                .withObservedGeneration(generation(primary))
                .withReason(clusterCondition.reason())
                .withStatus(clusterCondition.status())
                .withType(clusterCondition.type())
                .build();
    }

    private static boolean isTransition(@Nullable Condition oldReady, @Nullable Exception exception) {
        if (oldReady == null) {
            return true;
        }
        else if (isReadyEqualsTrue(oldReady)) {
            return exception != null; // a transition iff there's now an error
        }
        else { // there was a previous error
            if (exception == null) {
                return true; // => a previous error has been fixed
            }
            else {
                // => a transition iff the errors are different
                return !Objects.equals(oldReady.getMessage(), conditionMessage(exception))
                        || !Objects.equals(oldReady.getReason(), conditionReason(exception));
            }
        }
    }

    private static String conditionMessage(@Nullable Exception exception) {
        return exception == null ? "" : exception.getMessage();
    }

    @NonNull
    private static String conditionReason(@Nullable Exception exception) {
        if (exception == null) {
            return "";
        }
        else {
            return exception.getClass().getSimpleName();
        }
    }

    private static boolean isReadyEqualsTrue(Condition oldReady) {
        return Condition.Status.TRUE.equals(oldReady.getStatus());
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
    static @NonNull SecondaryToPrimaryMapper<KafkaService> kafkaServiceRefToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return kafkaServiceRef -> {
            // find all virtual clusters that reference this kafkaServiceRef

            var proxyRefs = ResourcesUtil.resourcesInSameNamespace(context, kafkaServiceRef, VirtualKafkaCluster.class)
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
    static @NonNull PrimaryToSecondaryMapper<HasMetadata> proxyToKafkaServiceMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            // Load all the virtual clusters for the KafkaProxy, then extract all the referenced KafkaService resource ids.
            var clusterRefs = ResourcesUtil.resourcesInSameNamespace(context, primary, VirtualKafkaCluster.class)
                    .filter(vkc -> vkc.getSpec().getProxyRef().equals(toLocalRef(primary)))
                    .map(VirtualKafkaCluster::getSpec)
                    .map(VirtualKafkaClusterSpec::getTargetKafkaServiceRef)
                    .collect(Collectors.toSet());

            Set<ResourceID> kafkaServiceRefs = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaService.class,
                    cluster -> clusterRefs.contains(toLocalRef(cluster)));
            LOGGER.debug("Event source KafkaService PrimaryToSecondaryMapper got {}", kafkaServiceRefs);
            return kafkaServiceRefs;
        };
    }

    @NonNull
    private static InformerEventSource<KafkaProtocolFilter, KafkaProxy> eventSourceForFilter(EventSourceContext<KafkaProxy> context) {

        var configuration = InformerEventSourceConfiguration.from(KafkaProtocolFilter.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(filterToProxy(context))
                .withPrimaryToSecondaryMapper(proxyToFilters(context))
                .build();

        return new InformerEventSource<>(configuration, context);
    }

    @VisibleForTesting
    static @NonNull PrimaryToSecondaryMapper<KafkaProxy> proxyToFilters(EventSourceContext<KafkaProxy> context) {
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
    static @NonNull PrimaryToSecondaryMapper<HasMetadata> proxyToClusterMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            Set<ResourceID> virtualClustersInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, VirtualKafkaCluster.class,
                    clusterReferences(primary));
            LOGGER.debug("Event source VirtualKafkaCluster PrimaryToSecondaryMapper got {}", virtualClustersInProxyNamespace);
            return virtualClustersInProxyNamespace;
        };
    }

    @VisibleForTesting
    static @NonNull SecondaryToPrimaryMapper<VirtualKafkaCluster> clusterToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return cluster -> {
            // we need to reconcile all proxies when a virtual kafka cluster changes in case the proxyRef is updated, we need to update
            // the previously referenced proxy too.
            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, cluster, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source VirtualKafkaCluster SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    @VisibleForTesting
    static @NonNull SecondaryToPrimaryMapper<KafkaProxyIngress> ingressToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return ingress -> {
            // we need to reconcile all proxies when a kafka proxy ingress changes in case the proxyRef is updated, we need to update
            // the previously referenced proxy too.
            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, ingress, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source KafkaProxyIngress SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    @VisibleForTesting
    static @NonNull PrimaryToSecondaryMapper<KafkaProxy> proxyToIngressMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            Set<ResourceID> ingressesInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaProxyIngress.class,
                    ingressReferences(primary));
            LOGGER.debug("Event source KafkaProxyIngress PrimaryToSecondaryMapper got {}", ingressesInProxyNamespace);
            return ingressesInProxyNamespace;
        };
    }

    @VisibleForTesting
    static @NonNull SecondaryToPrimaryMapper<KafkaProtocolFilter> filterToProxy(EventSourceContext<KafkaProxy> context) {
        return (KafkaProtocolFilter filter) -> {
            // filters don't point to a proxy, but must be in the same namespace as the proxy/proxies which reference the,
            // so when a filter changes we reconcile all the proxies in the same namespace
            Set<ResourceID> proxiesInFilterNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, filter, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source SecondaryToPrimaryMapper got {}", proxiesInFilterNamespace);
            return proxiesInFilterNamespace;
        };
    }

    private static @NonNull Predicate<VirtualKafkaCluster> clusterReferences(HasMetadata primary) {
        return cluster -> cluster.getSpec().getProxyRef().getName().equals(name(primary));
    }

    private static @NonNull Predicate<KafkaProxyIngress> ingressReferences(HasMetadata primary) {
        return ingress -> ingress.getSpec().getProxyRef().getName().equals(name(primary));
    }
}
