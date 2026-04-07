/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxyingress;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.openshift.api.model.Route;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourceState;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Reconciles a {@link KafkaProxyIngress} resource.
 * <br/>
 * It does this by checking the following:
 * <ol>
 *     <li>It checks whether the the {@link KafkaProxy} referenced by the {@code spec.proxyRef.name} actually exists.</li>
 *     <li>It checks whether the specified ingress are accepted for the platform. This check currently  applies to the {@code spec.openShiftRoute} ingress type only.
 *     If this ingress type is specified the platform must provide the OpenShift Route API.</li>
 * </ol>
 * The reconciler uses the {@link Condition.Type#ResolvedRefs} {@link Condition} to report if the KafkaProxy spec is present.
 * The reconciler uses the {@link Condition.Type#Accepted} {@link Condition} to report the ingress is fully acceptable.
 * This is the case if the proxy exists and any specific ingress requirement passes.
 */
public class KafkaProxyIngressReconciler implements
        Reconciler<KafkaProxyIngress> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyIngressReconciler.class);
    public static final String PROXY_EVENT_SOURCE_NAME = "proxy";
    private final KafkaProxyIngressStatusFactory statusFactory;

    public KafkaProxyIngressReconciler(Clock clock) {
        this.statusFactory = new KafkaProxyIngressStatusFactory(Objects.requireNonNull(clock));
    }

    @Override
    public List<EventSource<?, KafkaProxyIngress>> prepareEventSources(EventSourceContext<KafkaProxyIngress> context) {
        InformerEventSourceConfiguration<KafkaProxy> configuration = InformerEventSourceConfiguration.from(
                KafkaProxy.class,
                KafkaProxyIngress.class)
                .withName(PROXY_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((KafkaProxyIngress ingress) -> ResourcesUtil.localRefAsResourceId(ingress, ingress.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(proxy -> ResourcesUtil.findReferrers(context,
                        proxy,
                        KafkaProxyIngress.class,
                        ingress -> Optional.of(ingress.getSpec().getProxyRef())))
                .build();
        return List.of(new InformerEventSource<>(configuration, context));
    }

    @Override
    public UpdateControl<KafkaProxyIngress> reconcile(
                                                      KafkaProxyIngress ingress,
                                                      Context<KafkaProxyIngress> context)
            throws Exception {

        var proxyOpt = context.getSecondaryResource(KafkaProxy.class, PROXY_EVENT_SOURCE_NAME);
        LOGGER.atDebug()
                .addKeyValue(OperatorLoggingKeys.TO, proxyOpt)
                .log("Resolved spec.proxyRef.name");

        var isIngressSpecUsingOpenshiftRoute = ingress.getSpec().getOpenShiftRoute() != null;
        var isOpenShiftRouteApiAvailable = context.getClient().supports(Route.class);

        var conditions = new ArrayList<Condition>();

        addResolvedRefCondition(ingress, proxyOpt, conditions);
        addAcceptedConditions(ingress, isIngressSpecUsingOpenshiftRoute, isOpenShiftRouteApiAvailable, proxyOpt, conditions);

        var update = statusFactory.ingressStatusPatch(ingress, ResourceState.fromList(conditions), MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
        UpdateControl<KafkaProxyIngress> updateControl;
        if (update.getStatus().getConditions().stream().allMatch(c -> Condition.Status.TRUE.equals(c.getStatus()))) {
            updateControl = UpdateControl.patchResourceAndStatus(update);
        }
        else {
            updateControl = UpdateControl.patchStatus(update);
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(ingress))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(ingress))
                    .log("Completed reconciliation");
        }

        return updateControl;
    }

    private void addAcceptedConditions(KafkaProxyIngress ingress,
                                       boolean isIngressSpecUsingOpenshiftRoute,
                                       boolean isOpenShiftRouteApiAvailable,
                                       Optional<KafkaProxy> proxyOpt,
                                       List<Condition> conditions) {
        if (isIngressSpecUsingOpenshiftRoute && !isOpenShiftRouteApiAvailable) {
            conditions.add(statusFactory.newFalseCondition(
                    ingress,
                    Condition.Type.Accepted,
                    Condition.REASON_REQUESTED_RESOURCE_KIND_NOT_SUPPORTED,
                    "Kubernetes server is missing support for resource kind Route. spec.openShiftRoute is only supported on OpenShift."));
        }
        else if (proxyOpt.isEmpty()) {
            conditions.add(statusFactory.newFalseCondition(
                    ingress,
                    Condition.Type.Accepted,
                    Condition.REASON_INVALID,
                    "Other conditions prevent the resource being acceptable."));
        }
        else {
            conditions.add(statusFactory.newTrueCondition(
                    ingress,
                    Condition.Type.Accepted));
        }
    }

    private void addResolvedRefCondition(KafkaProxyIngress ingress,
                                         Optional<KafkaProxy> proxyOpt,
                                         List<Condition> conditions) {
        if (proxyOpt.isEmpty()) {
            conditions.add(statusFactory.newFalseCondition(
                    ingress,
                    Condition.Type.ResolvedRefs,
                    Condition.REASON_REFS_NOT_FOUND,
                    "KafkaProxy spec.proxyRef.name not found"));
        }
        else {
            conditions.add(statusFactory.newTrueCondition(
                    ingress, Condition.Type.ResolvedRefs));
        }
    }

    @Override
    public ErrorStatusUpdateControl<KafkaProxyIngress> updateErrorStatus(
                                                                         KafkaProxyIngress ingress,
                                                                         Context<KafkaProxyIngress> context,
                                                                         Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<KafkaProxyIngress> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(ingress, Condition.Type.ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(ingress))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(ingress))
                    .addKeyValue(OperatorLoggingKeys.ERROR, e.toString())
                    .log("Completed reconciliation with error");
        }
        return uc;
    }
}
