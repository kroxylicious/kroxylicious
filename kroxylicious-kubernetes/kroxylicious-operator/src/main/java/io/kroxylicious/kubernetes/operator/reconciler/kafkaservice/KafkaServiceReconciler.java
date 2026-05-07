/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.kubernetes.api.common.CertificateRef;
import io.kroxylicious.kubernetes.api.common.CipherSuites;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.Protocols;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourceCheckResult;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.common.Condition.Type.ResolvedRefs;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.retrieveBootstrapServerAddress;

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
    public static final String CONFIG_MAPS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME = "configmapsTrustAnchorRef";
    public static final String SECRETS_STRIMZI_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME = "secretsStrimziTrustAnchorRef";
    public static final String SECRETS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME = "secretsTrustAnchorRef";
    public static final String STRIMZI_KAFKA_EVENT_SOURCE_NAME = "kafkas";

    private static final String SPEC_REF = "spec.strimziKafkaRef";
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
                .withPrimaryToSecondaryMapper(new KafkaServicePrimaryToSecretSecondaryJoinedOnTlsCertificateRefMapper())
                .withSecondaryToPrimaryMapper(new SecretSecondaryJoinedOnTlsCertificateRefMapperToKafkaServicePrimaryMapper(context))
                .build();

        InformerEventSourceConfiguration<ConfigMap> serviceToConfigMapTrustAnchorRef = InformerEventSourceConfiguration.from(
                ConfigMap.class,
                KafkaService.class)
                .withName(CONFIG_MAPS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(new KafkaServicePrimaryToResourceSecondaryJoinedOnTlsTrustAnchorRefMapper())
                .withSecondaryToPrimaryMapper(new ConfigMapSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(context))
                .build();

        InformerEventSourceConfiguration<Secret> serviceToSecretTrustAnchorRef = InformerEventSourceConfiguration.from(
                Secret.class,
                KafkaService.class)
                .withName(SECRETS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(new KafkaServicePrimaryToResourceSecondaryJoinedOnTlsTrustAnchorRefMapper())
                .withSecondaryToPrimaryMapper(new SecretSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(context))
                .build();

        InformerEventSourceConfiguration<Secret> serviceToStrimziCaCertificate = InformerEventSourceConfiguration.from(
                Secret.class,
                KafkaService.class)
                .withName(SECRETS_STRIMZI_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(new KafkaServicePrimaryToStrimziCaCertificateSecondaryMapper())
                .withSecondaryToPrimaryMapper(new StrimziCaCertificateSecondaryToKafkaServicePrimaryMapper(context))
                .build();

        List<EventSource<?, KafkaService>> informersList = new ArrayList<>();

        informersList.add(new InformerEventSource<>(serviceToSecret, context));
        informersList.add(new InformerEventSource<>(serviceToConfigMapTrustAnchorRef, context));
        informersList.add(new InformerEventSource<>(serviceToSecretTrustAnchorRef, context));

        if (context.getClient().supports(Kafka.class)) {
            LOGGER.atDebug()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, context.getClient().getNamespace())
                    .log("Adding kafkas.strimzi.io.kafka informer because the Kafka CRD is supported by the cluster");
            InformerEventSourceConfiguration<Kafka> serviceToStrimziKafka = InformerEventSourceConfiguration.from(
                    Kafka.class,
                    KafkaService.class)
                    .withName(STRIMZI_KAFKA_EVENT_SOURCE_NAME)
                    .withPrimaryToSecondaryMapper(new KafkaServicePrimaryToStrimziKafkaSecondaryMapper())
                    .withSecondaryToPrimaryMapper(new StrimziKafkaSecondaryToKafkaServicePrimaryMapper(context))
                    .build();
            informersList.add(new InformerEventSource<>(serviceToStrimziKafka, context));
            informersList.add(new InformerEventSource<>(serviceToStrimziCaCertificate, context));
        }

        return informersList;
    }

    @Override
    public UpdateControl<KafkaService> reconcile(KafkaService service, Context<KafkaService> context) {
        // Validate StrimziKafkaRef if present
        ValidationResult strimziValidation = validateStrimziKafkaRef(service, context);
        if (strimziValidation.hasFailed()) {
            return logAndReturnUpdateControl(service, strimziValidation.failedService());
        }

        // Resolve trust anchor (explicit or auto-discovered)
        TrustAnchorResolution trustResolution = resolveTrustAnchor(service, context, strimziValidation.referents());
        if (trustResolution.hasFailed()) {
            return logAndReturnUpdateControl(service, trustResolution.failedService());
        }

        // Validate certificate ref if present
        ValidationResult certValidation = validateCertificateRef(service, context, trustResolution.referents());
        if (certValidation.hasFailed()) {
            return logAndReturnUpdateControl(service, certValidation.failedService());
        }

        // Build success status with all resolved information
        KafkaService updated = buildSuccessStatus(service, context, certValidation.referents(), trustResolution.trustAnchorRef());

        return logAndReturnUpdateControl(service, updated);
    }

    private ValidationResult validateStrimziKafkaRef(KafkaService service, Context<KafkaService> context) {
        var strimziKafkaRefOpt = Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getStrimziKafkaRef);

        if (strimziKafkaRefOpt.isEmpty()) {
            return ValidationResult.success();
        }

        ResourceCheckResult<KafkaService> result = ResourcesUtil.checkStrimziKafkaRef(
                service, context, STRIMZI_KAFKA_EVENT_SOURCE_NAME,
                strimziKafkaRefOpt.get(), SPEC_REF, statusFactory);

        return result.resource() != null
                ? ValidationResult.failure(result.resource())
                : ValidationResult.success(result.referents());
    }

    private TrustAnchorResolution resolveTrustAnchor(
                                                     KafkaService service,
                                                     Context<KafkaService> context,
                                                     List<HasMetadata> existingReferents) {

        var strimziKafkaRefOpt = Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getStrimziKafkaRef);
        var trustAnchorRefOpt = Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(Tls::getTrustAnchorRef);

        // Case 1: Explicit trust anchor ref (always takes precedence when present)
        if (trustAnchorRefOpt.isPresent()) {
            return resolveExplicitTrustAnchor(service, context, trustAnchorRefOpt.get(), existingReferents);
        }

        // Case 2: Auto-discovered Strimzi CA certificate
        else if (isUsingStrimziCaTrust(strimziKafkaRefOpt)) {
            return resolveStrimziCaTrust(service, context, strimziKafkaRefOpt.get(), existingReferents);
        }

        // Case 3: No trust anchor
        else {
            return TrustAnchorResolution.noTrustAnchor(existingReferents);
        }
    }

    private boolean isUsingStrimziCaTrust(Optional<io.kroxylicious.kubernetes.api.common.StrimziKafkaRef> strimziRefOpt) {
        return strimziRefOpt.isPresent() && strimziRefOpt.get().getTrustStrimziCaCertificate();
    }

    private TrustAnchorResolution resolveExplicitTrustAnchor(
                                                             KafkaService service,
                                                             Context<KafkaService> context,
                                                             TrustAnchorRef trustAnchorRef,
                                                             List<HasMetadata> existingReferents) {

        String eventSourceName = trustAnchorRef.getRef().getKind() != null &&
                Objects.equals(trustAnchorRef.getRef().getKind(), "Secret")
                        ? SECRETS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME
                        : CONFIG_MAPS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME;

        ResourceCheckResult<KafkaService> result = ResourcesUtil.checkTrustAnchorRef(
                service, context, eventSourceName, trustAnchorRef,
                SPEC_TLS_TRUST_ANCHOR_REF, statusFactory);

        if (result.resource() != null) {
            return TrustAnchorResolution.failure(result.resource());
        }

        List<HasMetadata> allReferents = combineReferents(existingReferents, result.referents());
        String storeType = trustAnchorRef.getStoreType() != null
                ? trustAnchorRef.getStoreType()
                : ResourcesUtil.deriveStoreTypeFromKeySuffix(trustAnchorRef);

        TrustAnchorRef resolvedRef = new TrustAnchorRefBuilder()
                .withNewRef()
                .withName(trustAnchorRef.getRef().getName())
                .withKind(trustAnchorRef.getRef().getKind())
                .endRef()
                .withKey(trustAnchorRef.getKey())
                .withStoreType(storeType)
                .build();

        return TrustAnchorResolution.success(resolvedRef, allReferents);
    }

    private TrustAnchorResolution resolveStrimziCaTrust(
                                                        KafkaService service,
                                                        Context<KafkaService> context,
                                                        io.kroxylicious.kubernetes.api.common.StrimziKafkaRef strimziRef,
                                                        List<HasMetadata> existingReferents) {

        ResourceCheckResult<KafkaService> result = ResourcesUtil.checkStrimziTrustAnchor(
                service, context, strimziRef, statusFactory);

        if (result.resource() != null) {
            return TrustAnchorResolution.failure(result.resource());
        }

        List<HasMetadata> allReferents = combineReferents(existingReferents, result.referents());
        TrustAnchorRef resolvedRef = new TrustAnchorRefBuilder()
                .withNewRef()
                .withName(strimziRef.getRef().getName() + STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX)
                .withKind("Secret")
                .endRef()
                .withKey(ResourcesUtil.STRIMZI_CLUSTER_CA_BUNDLE)
                .withStoreType(ResourcesUtil.STRIMZI_CLUSTER_CA_STORE_TYPE)
                .build();

        return TrustAnchorResolution.success(resolvedRef, allReferents);
    }

    private ValidationResult validateCertificateRef(
                                                    KafkaService service,
                                                    Context<KafkaService> context,
                                                    List<HasMetadata> existingReferents) {

        var certRefOpt = Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(Tls::getCertificateRef);

        if (certRefOpt.isEmpty()) {
            return ValidationResult.success(existingReferents);
        }

        ResourceCheckResult<KafkaService> result = ResourcesUtil.checkCertRef(
                service, certRefOpt.get(), SPEC_TLS_CERTIFICATE_REF,
                statusFactory, context, SECRETS_EVENT_SOURCE_NAME);

        return result.resource() != null
                ? ValidationResult.failure(result.resource())
                : ValidationResult.success(combineReferents(existingReferents, result.referents()));
    }

    private KafkaService buildSuccessStatus(
                                            KafkaService service,
                                            Context<KafkaService> context,
                                            List<HasMetadata> allReferents,
                                            @Nullable TrustAnchorRef trustAnchorRef) {

        String checksum = computeChecksum(allReferents);

        var specTls = Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getTls)
                .orElse(null);

        var statusTls = buildStatusTls(
                trustAnchorRef,
                specTls != null ? specTls.getCertificateRef() : null,
                specTls != null ? specTls.getProtocols() : null,
                specTls != null ? specTls.getCipherSuites() : null);

        if (service.getSpec().getStrimziKafkaRef() != null) {
            return buildStrimziBasedStatus(service, context, checksum, statusTls);
        }
        else {
            return statusFactory.newTrueConditionStatusPatch(
                    service, ResolvedRefs, checksum,
                    service.getSpec().getBootstrapServers(), statusTls);
        }
    }

    private String computeChecksum(List<HasMetadata> referents) {
        var checksumGenerator = new Crc32ChecksumGenerator();
        referents.forEach(checksumGenerator::appendMetadata);
        return checksumGenerator.encode();
    }

    @Nullable
    private io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicestatus.Tls buildStatusTls(
                                                                                          @Nullable TrustAnchorRef trustAnchorRef,
                                                                                          @Nullable CertificateRef certificateRef,
                                                                                          @Nullable Protocols protocols,
                                                                                          @Nullable CipherSuites cipherSuites) {

        if (trustAnchorRef == null && certificateRef == null && protocols == null && cipherSuites == null) {
            return null;
        }

        return new io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicestatus.TlsBuilder()
                .withTrustAnchorRef(trustAnchorRef)
                .withCertificateRef(certificateRef)
                .withProtocols(protocols)
                .withCipherSuites(cipherSuites)
                .build();
    }

    private KafkaService buildStrimziBasedStatus(
                                                 KafkaService service,
                                                 Context<KafkaService> context,
                                                 String checksum,
                                                 @Nullable io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicestatus.Tls statusTls) {

        Optional<ListenerStatus> listenerStatus = retrieveBootstrapServerAddress(
                context, service, STRIMZI_KAFKA_EVENT_SOURCE_NAME);

        return listenerStatus
                .map(status -> statusFactory.newTrueConditionStatusPatch(
                        service, ResolvedRefs, checksum, status.getBootstrapServers(), statusTls))
                .orElseGet(() -> statusFactory.newFalseConditionStatusPatch(
                        service, ResolvedRefs,
                        Condition.REASON_REFERENCED_RESOURCE_NOT_RECONCILED,
                        "Referenced resource has not yet reconciled listener name: "
                                + service.getSpec().getStrimziKafkaRef().getListenerName()));
    }

    private List<HasMetadata> combineReferents(List<HasMetadata> existing, List<? extends HasMetadata> additional) {
        List<HasMetadata> combined = new ArrayList<>(existing);
        combined.addAll(additional);
        return combined;
    }

    private UpdateControl<KafkaService> logAndReturnUpdateControl(KafkaService service, KafkaService updated) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(service))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(service))
                    .log("Completed reconciliation");
        }
        return UpdateControl.patchResourceAndStatus(updated);
    }

    @Override
    public ErrorStatusUpdateControl<KafkaService> updateErrorStatus(KafkaService service, Context<KafkaService> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<KafkaService> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(service, ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(service))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(service))
                    .addKeyValue(OperatorLoggingKeys.ERROR, e.toString())
                    .log("Completed reconciliation with error");
        }
        return uc;
    }

    /**
     * Encapsulates validation outcome for a resource reference.
     */
    private record ValidationResult(
                                    @Nullable KafkaService failedService,
                                    List<HasMetadata> referents) {

        static ValidationResult success() {
            return new ValidationResult(null, List.of());
        }

        static ValidationResult success(List<? extends HasMetadata> refs) {
            return new ValidationResult(null, new ArrayList<>(refs));
        }

        static ValidationResult failure(KafkaService failed) {
            return new ValidationResult(failed, List.of());
        }

        boolean hasFailed() {
            return failedService != null;
        }
    }

    /**
     * Encapsulates trust anchor resolution outcome.
     */
    private record TrustAnchorResolution(
                                         @Nullable KafkaService failedService,
                                         @Nullable TrustAnchorRef trustAnchorRef,
                                         List<HasMetadata> referents) {

        static TrustAnchorResolution success(
                                             TrustAnchorRef trustRef,
                                             List<? extends HasMetadata> refs) {
            return new TrustAnchorResolution(null, trustRef, new ArrayList<>(refs));
        }

        static TrustAnchorResolution noTrustAnchor(List<? extends HasMetadata> refs) {
            return new TrustAnchorResolution(null, null, new ArrayList<>(refs));
        }

        static TrustAnchorResolution failure(KafkaService failed) {
            return new TrustAnchorResolution(failed, null, List.of());
        }

        boolean hasFailed() {
            return failedService != null;
        }
    }
}
