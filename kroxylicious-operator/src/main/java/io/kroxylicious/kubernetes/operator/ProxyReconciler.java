/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.AggregatedOperatorException;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.ConditionsBuilder;

import edu.umd.cs.findbugs.annotations.Nullable;

// @formatter:off
@ControllerConfiguration(dependents = {
        @Dependent(
                name = ProxyReconciler.CONFIG_DEP,
                type = ProxyConfigSecret.class),
        @Dependent(
                name = ProxyReconciler.DEPLOYMENT_DEP,
                type = ProxyDeployment.class,
                dependsOn = { ProxyReconciler.CONFIG_DEP },
                readyPostcondition = DeploymentReadyCondition.class
        ),
        @Dependent(
                name = ProxyReconciler.METRICS_DEP,
                type = MetricsService.class,
                dependsOn = { ProxyReconciler.DEPLOYMENT_DEP }
                //useEventSourceWithName = ProxyReconciler.METRICS_DEP
        ),
        @Dependent(
                name = ProxyReconciler.CLUSTERS_DEP,
                type = ClusterService.class,
                dependsOn = { ProxyReconciler.DEPLOYMENT_DEP }
                //useEventSourceWithName = ProxyReconciler.CLUSTERS_DEP
        )
})
// @formatter:on
public class ProxyReconciler implements
        Reconciler<KafkaProxy>,
        ErrorStatusHandler<KafkaProxy>,
        EventSourceInitializer<KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconciler.class);

    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
    public static final String METRICS_DEP = "metrics";
    public static final String CLUSTERS_DEP = "clusters";

    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        return UpdateControl.patchStatus(
                buildStatus(primary, null));
    }

    @Override
    public ErrorStatusUpdateControl<KafkaProxy> updateErrorStatus(KafkaProxy primary,
                                                                  Context<KafkaProxy> context,
                                                                  Exception exception) {
        // Post-condition: status.conditions should be in a canonical order (to avoid non-terminating reconciliations)
        // Post-condition: There is only one Ready condition
        var control = ErrorStatusUpdateControl.patchStatus(buildStatus(primary, exception));
        if (exception instanceof InvalidResourceException) {
            control.withNoRetry();
        }
        else {
            control.rescheduleAfter(Duration.ofSeconds(10));
        }
        return control;

    }

    private static KafkaProxy buildStatus(KafkaProxy primary,
                                          @Nullable Exception exception) {
        // @formatter:off
        return new KafkaProxyBuilder(primary)
                .editOrNewStatus()
                    .withObservedGeneration(primary.getMetadata().getGeneration())
                    .withConditions(effectiveReadyCondition(primary, exception))
                .endStatus()
            .build();
        // @formatter:on
    }

    /**
     * Determines whether the {@code Ready} condition has had a state transition,
     * and returns an appropriate {@code Ready} condition.
     * @param primary The primary.
     * @param exception An exception, or null if the reconciliation was successful.
     * @return The {@code Ready} condition to use in {@code status.conditions}.
     */
    private static Conditions effectiveReadyCondition(KafkaProxy primary,
                                                      @Nullable Exception exception) {
        final var oldReady = primary.getStatus() == null || primary.getStatus().getConditions() == null
                ? null
                : primary.getStatus().getConditions().stream().filter(c -> "Ready".equals(c.getType())).findFirst().orElse(null);

        Conditions newReady = newReadyCondition(primary, exception);

        final boolean useNew;

        if (oldReady == null) {
            useNew = true;
        }
        else if (Conditions.Status.TRUE.equals(oldReady.getStatus()) && exception == null) {
            // Both Ready=True
            useNew = false;
        }
        else if (Conditions.Status.TRUE.equals(oldReady.getStatus()) && exception != null) {
            // Ready=True transition to Ready=False
            useNew = true;
        }
        else if (exception == null) {
            // Ready=False transition to Ready=True
            useNew = true;
        }
        else {
            // Both Ready=False
            // Count it as a transition if the exception or message are different
            // Otherwise just update the observedGeneration
            useNew = !oldReady.getReason().equals(newReady.getReason())
                    || !oldReady.getMessage().equals(newReady.getMessage());
        }

        if (exception != null && (oldReady == null || useNew)) {
            // reduce verbosity by only logging if we're making a transition
            logException(primary, exception);
        }

        if (useNew) {
            return newReady;
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
                    .log("Operator observed an invalid resource");
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
    private static Conditions newReadyCondition(KafkaProxy primary, @Nullable Exception exception) {
        if (exception instanceof AggregatedOperatorException aoe && aoe.getAggregatedExceptions().size() == 1) {
            exception = aoe.getAggregatedExceptions().values().iterator().next();
        }

        return new ConditionsBuilder()
                .withLastTransitionTime(ZonedDateTime.now(ZoneId.of("Z")))
                .withMessage(exception == null ? "" : exception.getMessage())
                .withObservedGeneration(primary.getMetadata().getGeneration())
                .withReason(exception == null ? "" : exception.getClass().getSimpleName())
                .withStatus(exception == null ? Conditions.Status.TRUE : Conditions.Status.FALSE)
                .withType("Ready")
                .build();
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
                                                        EventSourceContext<KafkaProxy> context) {

        InformerEventSource<Service, KafkaProxy> ies1 = new InformerEventSource<>(
                InformerConfiguration.from(Service.class, context)
                        .build(),
                context);

        InformerEventSource<Service, KafkaProxy> ies2 = new InformerEventSource<>(
                InformerConfiguration.from(Service.class, context)
                        .build(),
                context);

        return Map.of(ClusterService.class.getName(), ies1,
                MetricsService.class.getName(), ies2);
    }
}
