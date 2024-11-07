/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.javaoperatorsdk.operator.AggregatedOperatorException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.ConditionsBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;
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
                name = ProxyReconciler.CLUSTERS_DEP,
                type = ClusterService.class,
                dependsOn = { ProxyReconciler.DEPLOYMENT_DEP }
                //useEventSourceWithName = ProxyReconciler.CLUSTERS_DEP
        )
})
// @formatter:on
public class ProxyReconciler implements
        Reconciler<KafkaProxy>,
        ErrorStatusHandler<KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconciler.class);

    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
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
            return newReadyCondition(primary, exception);
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
    private static Conditions newReadyCondition(KafkaProxy primary, @Nullable Exception exception) {
        if (exception instanceof AggregatedOperatorException aoe && aoe.getAggregatedExceptions().size() == 1) {
            exception = aoe.getAggregatedExceptions().values().iterator().next();
        }

        return new ConditionsBuilder()
                .withLastTransitionTime(ZonedDateTime.now(ZoneId.of("Z")))
                .withMessage(conditionMessage(exception))
                .withObservedGeneration(primary.getMetadata().getGeneration())
                .withReason(conditionReason(exception))
                .withStatus(exception == null ? Conditions.Status.TRUE : Conditions.Status.FALSE)
                .withType("Ready")
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
        return exception == null ? "" : exception.getClass().getSimpleName();
    }

    private static boolean isReadyEqualsTrue(Conditions oldReady) {
        return Conditions.Status.TRUE.equals(oldReady.getStatus());
    }
}
