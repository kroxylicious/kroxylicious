/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.kubernetes.api.common.Condition.Type.ResolvedRefs;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * <p>Reconciles a {@link KafkaService} by checking whether resources referred to in {@code spec.tls.certificateRef}
 * and/or {@code spec.tls.trustAnchorRef} actually exist, setting a {@link Condition.Type#ResolvedRefs} {@link Condition}
 * accordingly.</p>
 *
 * <p>Because a service CR is not uniquely associated with a {@code KafkaProxy} it's not possible
 * to set an {@code Accepted} condition on CR instances.</p>
 */
public final class KafkaServiceReconciler implements
        io.javaoperatorsdk.operator.api.reconciler.Reconciler<KafkaService> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceReconciler.class);

    public static final String SECRETS_EVENT_SOURCE_NAME = "secrets";
    public static final String CONFIG_MAPS_EVENT_SOURCE_NAME = "configmaps";
    private static final String SPEC_TLS_TRUST_ANCHOR_REF = "spec.tls.trustAnchorRef";
    private static final String SPEC_TLS_CERTIFICATE_REF = "spec.tls.certificateRef";

    private final KafkaServiceStatusFactory statusFactory;

    public KafkaServiceReconciler(Clock clock) {
        this.statusFactory = new KafkaServiceStatusFactory(clock);
    }

    @Override
    public List<EventSource<?, KafkaService>> prepareEventSources(EventSourceContext<KafkaService> context) {
        InformerEventSourceConfiguration<Secret> serviceToSecret = InformerEventSourceConfiguration.from(
                Secret.class,
                KafkaService.class)
                .withName(SECRETS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(kafkaServiceToSecret())
                .withSecondaryToPrimaryMapper(secretToKafkaService(context))
                .build();
        InformerEventSourceConfiguration<ConfigMap> serviceToConfigMap = InformerEventSourceConfiguration.from(
                ConfigMap.class,
                KafkaService.class)
                .withName(CONFIG_MAPS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(kafkaServiceToConfigMap())
                .withSecondaryToPrimaryMapper(configMapToKafkaService(context))
                .build();
        return List.of(
                new InformerEventSource<>(serviceToSecret, context),
                new InformerEventSource<>(serviceToConfigMap, context));
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaService> kafkaServiceToSecret() {
        return (KafkaService cluster) -> Optional.ofNullable(cluster.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(Tls::getCertificateRef)
                .map(cr -> ResourcesUtil.localRefAsResourceId(cluster, cr)).orElse(Set.of());
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<Secret> secretToKafkaService(EventSourceContext<KafkaService> context) {
        return secret -> ResourcesUtil.findReferrers(context,
                secret,
                KafkaService.class,
                service -> Optional.ofNullable(service.getSpec())
                        .map(KafkaServiceSpec::getTls)
                        .map(Tls::getCertificateRef));
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<ConfigMap> configMapToKafkaService(EventSourceContext<KafkaService> context) {
        return configMap -> ResourcesUtil.findReferrers(context,
                configMap,
                KafkaService.class,
                service -> Optional.ofNullable(service.getSpec())
                        .map(KafkaServiceSpec::getTls)
                        .map(Tls::getTrustAnchorRef)
                        .map(TrustAnchorRef::getRef));
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaService> kafkaServiceToConfigMap() {
        return (KafkaService cluster) -> Optional.ofNullable(cluster.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(Tls::getTrustAnchorRef)
                .map(tar -> ResourcesUtil.localRefAsResourceId(cluster, tar.getRef()))
                .orElse(Set.of());
    }

    @Override
    public UpdateControl<KafkaService> reconcile(KafkaService service, Context<KafkaService> context) {

        KafkaService updatedService = null;
        List<HasMetadata> referents = new ArrayList<>();
        var trustAnchorRefOpt = Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(Tls::getTrustAnchorRef);
        if (trustAnchorRefOpt.isPresent()) {
            ResourceCheckResult<KafkaService> result = ResourcesUtil.checkTrustAnchorRef(service, context, CONFIG_MAPS_EVENT_SOURCE_NAME, trustAnchorRefOpt.get(),
                    SPEC_TLS_TRUST_ANCHOR_REF,
                    statusFactory);
            updatedService = result.resource();
            referents.addAll(result.referents());
        }

        if (updatedService == null) {
            var certRefOpt = Optional.ofNullable(service.getSpec())
                    .map(KafkaServiceSpec::getTls)
                    .map(Tls::getCertificateRef);
            if (certRefOpt.isPresent()) {
                ResourceCheckResult<KafkaService> result = ResourcesUtil.checkCertRef(service, certRefOpt.get(), SPEC_TLS_CERTIFICATE_REF, statusFactory, context,
                        SECRETS_EVENT_SOURCE_NAME);
                updatedService = result.resource();
                referents.addAll(result.referents());
            }
        }

        if (updatedService == null) {
            var checksumGenerator = new Crc32ChecksumGenerator();
            for (HasMetadata metadataSource : referents) {
                checksumGenerator.appendMetadata(metadataSource);
            }

            updatedService = statusFactory.newTrueConditionStatusPatch(service, ResolvedRefs,
                    checksumGenerator.encode());
        }

        UpdateControl<KafkaService> uc = UpdateControl.patchResourceAndStatus(updatedService);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(service), name(service));
        }
        return uc;
    }

    @Override
    public ErrorStatusUpdateControl<KafkaService> updateErrorStatus(KafkaService service, Context<KafkaService> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<KafkaService> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(service, ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(service), name(service), e.toString());
        }
        return uc;
    }
}
