/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.javaoperatorsdk.operator.AggregatedOperatorException;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ContextInitializer;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.ConditionsBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

// @formatter:off
@ControllerConfiguration(dependents = {
        @Dependent(
                name = ProxyReconciler.CONFIG_DEP,
                type = ProxyConfigSecret.class
        ),
        @Dependent(
                name = ProxyReconciler.DEPLOYMENT_DEP,
                type = ProxyDeployment.class,
                dependsOn = { ProxyReconciler.CONFIG_DEP },
                readyPostcondition = DeploymentReadyCondition.class
        ),
        @Dependent(
                name = ProxyReconciler.CLUSTERS_DEP,
                type = ClusterService.class,
                dependsOn = { ProxyReconciler.DEPLOYMENT_DEP }
                //useEventSourceWithName = ProxyReconciler.CLUSTERS_DEP
        )
})
// @formatter:on
public class ProxyReconciler implements EventSourceInitializer<KafkaProxy>,
        ContextInitializer<KafkaProxy>,
        Reconciler<KafkaProxy>,
        ErrorStatusHandler<KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconciler.class);

    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
    public static final String CLUSTERS_DEP = "clusters";

    private final RuntimeDecl runtimeDecl;

    public ProxyReconciler(RuntimeDecl runtimeDecl) {
        this.runtimeDecl = runtimeDecl;
    }

    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        // var l = context.getSecondaryResourcesAsStream(RecordEncryption.class).toList();
        // LOGGER.info("Reconciled the PROXY {}, found associated filters {}", primary, l);
        LOGGER.info("Completed reconciliation of {}/{}", primary.getMetadata().getNamespace(), primary.getMetadata().getName());
        return UpdateControl.patchStatus(
                buildStatus(primary, context, null));
    }

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
        // @formatter:off
        List<Conditions> conditions = (primary.getSpec() == null || primary.getSpec().getClusters() == null ? Stream.<Clusters>empty() : primary.getSpec().getClusters().stream()).flatMap(cluster -> {
            return SharedKafkaProxyContext.clusterErrors(context, cluster).stream().map(ex ->
                    newCondition("InvalidCluster", primary, ex));
        }).collect(Collectors.toCollection(ArrayList::new));

        conditions.add(0, effectiveReadyCondition(primary, exception));

        return new KafkaProxyBuilder(primary)
                .editOrNewStatus()
                    .withObservedGeneration(primary.getMetadata().getGeneration())
                    .withConditions(conditions)
                .endStatus()
            .build();
        // @formatter:on
    }

    /**
     * Determines whether the {@code Ready} condition has had a state transition,
     * and returns an appropriate new {@code Ready} condition.
     * @param primary The primary.
     * @param exception An exception, or null if the reconciliation was successful.
     * @return The {@code Ready} condition to use in {@code status.conditions}.
     */
    private static Conditions effectiveReadyCondition(KafkaProxy primary,
                                                      @Nullable Exception exception) {
        final var oldReady = primary.getStatus() == null || primary.getStatus().getConditions() == null
                ? null
                : primary.getStatus().getConditions().stream().filter(c -> "Ready".equals(c.getType())).findFirst().orElse(null);

        if (isTransition(oldReady, exception)) {
            // reduce verbosity by only logging if we're making a transition
            if (exception != null) {
                logException(primary, exception);
            }
            return newCondition("Ready", primary, exception);
        }
        else {
            oldReady.setObservedGeneration(primary.getMetadata().getGeneration());
            return oldReady;
        }
    }

    static LoggingEventBuilder addResourceKeys(KafkaProxy primary, LoggingEventBuilder loggingEventBuilder) {
        return loggingEventBuilder.addKeyValue("kind", primary.getKind())
                .addKeyValue("group", primary.getGroup())
                .addKeyValue("namespace", primary.getMetadata().getNamespace())
                .addKeyValue("name", primary.getMetadata().getName());
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
     * @param primary The primary.
     * @param exception An exception, or null if the reconciliation was successful.
     * @return The {@code Ready} condition to use in {@code status.conditions}
     * <strong>if the condition had has a state transition</strong>.
     */
    private static Conditions newCondition(String conditionType, KafkaProxy primary, @Nullable Exception exception) {
        return new ConditionsBuilder()
                .withLastTransitionTime(ZonedDateTime.now(ZoneId.of("Z")))
                .withMessage(conditionMessage(exception))
                .withObservedGeneration(primary.getMetadata().getGeneration())
                .withReason(conditionReason(exception))
                .withStatus(exception == null ? Conditions.Status.TRUE : Conditions.Status.FALSE)
                .withType(conditionType)
                .build();
    }

    private static boolean isTransition(@Nullable Conditions oldReady, @Nullable Exception exception) {
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
        else if (exception instanceof InvalidClusterException ice) {
            return ice.reason();
        }
        else {
            return exception.getClass().getSimpleName();
        }
    }

    private static boolean isReadyEqualsTrue(Conditions oldReady) {
        return Conditions.Status.TRUE.equals(oldReady.getStatus());
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<KafkaProxy> context) {
        var eventSources = new ArrayList<InformerEventSource<GenericKubernetesResource, KafkaProxy>>(this.runtimeDecl.filterKindDecls().size());
        for (var filterKind : this.runtimeDecl.filterKindDecls()) {
            try {
                eventSources.add(eventSourceForFilter(context, filterKind));
            }
            catch (Exception e) {
                throw new OperatorConfigurationException("EventSource for " + filterKind + " could not be created.\n"
                        + "Hints:\n"
                        + "1. Check the Kind '" + filterKind.kind() + "' is present in the output of "
                        + "`kubectl api-resources --api-group=" + filterKind.group() + "`.\n"
                        + "2. Check access controls allow the operator to 'get,list,watch' this API.",
                        e);
            }
        }
        return EventSourceInitializer.nameEventSources(eventSources.toArray(new EventSource[0]));
    }

    @NonNull
    private static InformerEventSource<GenericKubernetesResource, KafkaProxy> eventSourceForFilter(
                                                                                                   EventSourceContext<KafkaProxy> context,
                                                                                                   FilterKindDecl filterKindDecl) {

        SecondaryToPrimaryMapper<GenericKubernetesResource> filterToProxy = (GenericKubernetesResource filter) -> {
            // filters don't point to a proxy, but must be in the same namespace as the proxy/proxies which reference the,
            // so when a filter changes we reconcile all the proxies in the same namespace
            Set<ResourceID> proxiesInFilterNamespace = context.getClient().resources(KafkaProxy.class)
                    .inNamespace(filter.getMetadata().getNamespace())
                    .list().getItems().stream()
                    .map(ResourceID::fromResource)
                    .collect(Collectors.toSet());
            LOGGER.info("Event source SecondaryToPrimaryMapper got {}", proxiesInFilterNamespace);
            return proxiesInFilterNamespace;
        };

        var configuration = InformerConfiguration.from(filterKindDecl.groupVersionKind(), context)
                .withSecondaryToPrimaryMapper(filterToProxy)
                .withPrimaryToSecondaryMapper((KafkaProxy proxy) -> proxyToFilterRefs(proxy, context))
                .build();

        return new InformerEventSource<>(configuration, context);
    }

    @NonNull
    private static Set<ResourceID> proxyToFilterRefs(KafkaProxy proxy, EventSourceContext<KafkaProxy> context) {
        var list = proxy.getSpec().getClusters().stream().toList();
        LOGGER.info("Event source PrimaryToSecondaryMapper got {}", list);
        Set<ResourceID> filterReferences = list.stream()
                .flatMap(cluster -> cluster.getFilters().stream())
                .map(filter -> {
                    ResourceID resourceID = new ResourceID(filter.getName(), proxy.getMetadata().getNamespace());
                    // context.getPrimaryCache().get(resourceID);
                    return resourceID;
                })
                .collect(Collectors.toSet());
        LOGGER.info("KafkaProxy {} has references to filters {}", ResourceID.fromResource(proxy), filterReferences);
        return filterReferences;
    }

    @Override
    public void initContext(
                            KafkaProxy primary,
                            Context<KafkaProxy> context) {
        SharedKafkaProxyContext.runtimeDecl(context, runtimeDecl);
    }
}
