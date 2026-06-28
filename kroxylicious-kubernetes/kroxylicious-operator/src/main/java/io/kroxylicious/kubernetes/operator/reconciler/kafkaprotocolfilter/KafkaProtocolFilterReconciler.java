/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaprotocolfilter;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerEventSource;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;

/**
 * <p>Reconciles a {@link KafkaProtocolFilter} by checking whether the {@link Secret}s
 * and {@link ConfigMap}s referred to in interpolated expressions actually exist, setting a
 * {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.</p>
 *
 * <p>Because a filter CR is not uniquely associated with a {@code KafkaProxy} it's not possible
 * to set an {@code Accepted} condition on CR instances.</p>
 */
public class KafkaProtocolFilterReconciler implements
        Reconciler<KafkaProtocolFilter> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProtocolFilterReconciler.class);
    private static final String SECRETS = "secrets";
    private static final String CONFIG_MAPS = "configmaps";
    private final KafkaProtocolFilterStatusFactory statusFactory;
    private final SecureConfigInterpolator secureConfigInterpolator;
    private final SharedInformerManager sharedInformerManager;

    public KafkaProtocolFilterReconciler(Clock clock, SecureConfigInterpolator secureConfigInterpolator, SharedInformerManager sharedInformerManager) {
        this.statusFactory = new KafkaProtocolFilterStatusFactory(Objects.requireNonNull(clock));
        this.secureConfigInterpolator = Objects.requireNonNull(secureConfigInterpolator);
        this.sharedInformerManager = Objects.requireNonNull(sharedInformerManager);
    }

    @Override
    public List<EventSource<?, KafkaProtocolFilter>> prepareEventSources(EventSourceContext<KafkaProtocolFilter> context) {
        // Get shared informers - all event sources of the same type share the same underlying cache
        var sharedSecretInformer = sharedInformerManager.getOrCreateInformer(Secret.class);
        var sharedConfigMapInformer = sharedInformerManager.getOrCreateInformer(ConfigMap.class);
        var allowedNamespaces = sharedInformerManager.effectiveNamespaces();

        var secretEventSource = createSharedInformerEventSource(
                context,
                Secret.class,
                SECRETS,
                sharedSecretInformer,
                interpolationResult -> interpolationResult.volumes().stream()
                        .flatMap(volume -> Optional.ofNullable(volume.getSecret())
                                .map(SecretVolumeSource::getSecretName)
                                .stream()),
                allowedNamespaces);

        var configMapEventSource = createSharedInformerEventSource(
                context,
                ConfigMap.class,
                CONFIG_MAPS,
                sharedConfigMapInformer,
                interpolationResult -> interpolationResult.volumes().stream()
                        .flatMap(volume -> Optional.ofNullable(volume.getConfigMap())
                                .map(ConfigMapVolumeSource::getName)
                                .stream()),
                allowedNamespaces);

        return List.of(secretEventSource, configMapEventSource);
    }

    /**
     * Creates a SharedInformerEventSource for tracking resource dependencies in filter config templates.
     *
     * @param context The event source context
     * @param resourceClass The Java type of resource reference (e.g. Secret, ConfigMap)
     * @param eventSourceName The name for the event source
     * @param sharedInformer The shared informer instance
     * @param resourceNameExtractor Function to extract resource names from interpolation results
     * @param allowedNamespaces Namespaces to filter events (empty means all namespaces)
     * @param <R> The type of referenced resource
     * @return A configured SharedInformerEventSource
     */
    private <R extends HasMetadata> SharedInformerEventSource<R, KafkaProtocolFilter> createSharedInformerEventSource(
                                                                                                                      EventSourceContext<KafkaProtocolFilter> context,
                                                                                                                      Class<R> resourceClass,
                                                                                                                      String eventSourceName,
                                                                                                                      SharedIndexInformer<R> sharedInformer,
                                                                                                                      Function<SecureConfigInterpolator.InterpolationResult, Stream<String>> resourceNameExtractor,
                                                                                                                      Set<String> allowedNamespaces) {
        var config = templateResourceReferenceEventSourceConfig(context, resourceClass, resourceNameExtractor);

        return new SharedInformerEventSource<>(
                resourceClass,
                eventSourceName,
                sharedInformer,
                config.getPrimaryToSecondaryMapper(),
                config.getSecondaryToPrimaryMapper(),
                allowedNamespaces);
    }

    /**
     * Returns a new event source config for getting the resource dependencies of a given type present in a filter's {@code spec.configTemplate}.
     * @param context The context.
     * @param secondaryClass The Java type of resource reference (e.g. Secret)
     * @param resourceNameExtractor A function which extracts the name of the resources from an interpolation result.
     * @param <R> The type of referenced resource
     * @return The event source configuration
     */
    private <R extends HasMetadata> InformerEventSourceConfiguration<R> templateResourceReferenceEventSourceConfig(
                                                                                                                   EventSourceContext<KafkaProtocolFilter> context,
                                                                                                                   Class<R> secondaryClass,
                                                                                                                   Function<SecureConfigInterpolator.InterpolationResult, Stream<String>> resourceNameExtractor) {
        return InformerEventSourceConfiguration.from(
                secondaryClass,
                KafkaProtocolFilter.class)
                .withPrimaryToSecondaryMapper((KafkaProtocolFilter filter) -> {
                    Object configTemplate = filter.getSpec().getConfigTemplate();
                    var interpolationResult = secureConfigInterpolator.interpolate(configTemplate);
                    Set<ResourceID> resourceIds = resourceNameExtractor.apply(interpolationResult)
                            .map(name -> new ResourceID(name, ResourcesUtil.namespace(filter)))
                            .collect(Collectors.toSet());
                    LOGGER.atDebug()
                            .addKeyValue("filterName", ResourcesUtil.name(filter))
                            .addKeyValue("secondaryClass", secondaryClass.getName())
                            .addKeyValue("resourceIds", resourceIds)
                            .log("Filter references secondary resources");
                    return resourceIds;
                })
                .withSecondaryToPrimaryMapper(secret -> {
                    Set<ResourceID> resourceIds = ResourcesUtil.filteredResourceIdsInSameNamespace(
                            context,
                            secret,
                            KafkaProtocolFilter.class,
                            filter -> {
                                Object configTemplate = filter.getSpec().getConfigTemplate();
                                var interpolationResult = secureConfigInterpolator.interpolate(configTemplate);
                                return resourceNameExtractor.apply(interpolationResult)
                                        .anyMatch(secretNameFromVolume -> secretNameFromVolume.equals(ResourcesUtil.name(secret)));
                            });
                    LOGGER.atDebug()
                            .addKeyValue("secondaryClass", secondaryClass.getName())
                            .addKeyValue("secretName", ResourcesUtil.name(secret))
                            .addKeyValue("resourceIds", resourceIds)
                            .log("Secondary resource referenced by Filters");
                    return resourceIds;
                })
                .build();
    }

    @Override
    public UpdateControl<KafkaProtocolFilter> reconcile(
                                                        KafkaProtocolFilter filter,
                                                        Context<KafkaProtocolFilter> context) {

        Map<String, Secret> existingSecretsByName = context.getSecondaryResourcesAsStream(Secret.class).collect(toByNameMap());
        LOGGER.atDebug()
                .addKeyValue("secrets", existingSecretsByName.keySet())
                .log("Existing secrets");

        Map<String, ConfigMap> existingConfigMapsByName = context.getSecondaryResourcesAsStream(ConfigMap.class).collect(toByNameMap());
        LOGGER.atDebug()
                .addKeyValue("configMaps", existingConfigMapsByName.keySet())
                .log("Existing config maps");

        var interpolationResult = secureConfigInterpolator.interpolate(filter.getSpec().getConfigTemplate());
        var referencedSecrets = interpolationResult.volumes().stream()
                .flatMap(volume -> Optional.ofNullable(volume.getSecret())
                        .map(SecretVolumeSource::getSecretName)
                        .stream())
                .collect(Collectors.toCollection(TreeSet::new));
        LOGGER.atDebug()
                .addKeyValue("secrets", referencedSecrets)
                .log("Referenced secrets");

        var referencedConfigMaps = interpolationResult.volumes().stream()
                .flatMap(volume -> Optional.ofNullable(volume.getConfigMap())
                        .map(ConfigMapVolumeSource::getName)
                        .stream())
                .collect(Collectors.toCollection(TreeSet::new));
        LOGGER.atDebug()
                .addKeyValue("configMaps", referencedConfigMaps)
                .log("Referenced config maps");

        KafkaProtocolFilter patch;
        if (existingSecretsByName.keySet().containsAll(referencedSecrets)
                && existingConfigMapsByName.keySet().containsAll(referencedConfigMaps)) {
            var checksumGenerator = new Crc32ChecksumGenerator();
            Stream<HasMetadata> referents = Stream.concat(referencedSecrets.stream().map(existingSecretsByName::get),
                    referencedConfigMaps.stream().map(existingConfigMapsByName::get));
            referents.forEachOrdered(checksumGenerator::appendMetadata);
            patch = statusFactory.newTrueConditionStatusPatch(
                    filter,
                    Condition.Type.ResolvedRefs,
                    checksumGenerator.encode());
        }
        else {
            referencedSecrets.removeAll(existingSecretsByName.keySet());
            referencedConfigMaps.removeAll(existingConfigMapsByName.keySet());
            String message = "Referenced";
            if (!referencedSecrets.isEmpty()) {
                message += " Secrets [" + String.join(", ", referencedSecrets) + "]";
            }
            if (!referencedConfigMaps.isEmpty()) {
                message += " ConfigMaps [" + String.join(", ", referencedConfigMaps) + "]";
            }
            message += " not found";
            patch = statusFactory.newFalseConditionStatusPatch(
                    filter,
                    Condition.Type.ResolvedRefs,
                    Condition.REASON_INTERPOLATED_REFS_NOT_FOUND,
                    message);
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(filter))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(filter))
                    .log("Completed reconciliation");
        }
        return UpdateControl.patchResourceAndStatus(patch);
    }

    @Override
    public ErrorStatusUpdateControl<KafkaProtocolFilter> updateErrorStatus(
                                                                           KafkaProtocolFilter filter,
                                                                           Context<KafkaProtocolFilter> context,
                                                                           Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<KafkaProtocolFilter> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(filter, Condition.Type.ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(filter))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(filter))
                    .addKeyValue(OperatorLoggingKeys.ERROR, e.toString())
                    .log("Completed reconciliation with error");
        }
        return uc;
    }
}
