/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

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
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

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
    private final KafkaProtocolFilterStatusFactory statusFactory;
    private final SecureConfigInterpolator secureConfigInterpolator;

    KafkaProtocolFilterReconciler(Clock clock, SecureConfigInterpolator secureConfigInterpolator) {
        this.statusFactory = new KafkaProtocolFilterStatusFactory(Objects.requireNonNull(clock));
        this.secureConfigInterpolator = Objects.requireNonNull(secureConfigInterpolator);
    }

    @Override
    public List<EventSource<?, KafkaProtocolFilter>> prepareEventSources(EventSourceContext<KafkaProtocolFilter> context) {
        return List.of(
                new InformerEventSource<>(templateResourceReferenceEventSourceConfig(context, Secret.class,
                        interpolationResult -> interpolationResult.volumes().stream()
                                .flatMap(volume -> Optional.ofNullable(volume.getSecret())
                                        .map(SecretVolumeSource::getSecretName)
                                        .stream())),
                        context),
                new InformerEventSource<>(templateResourceReferenceEventSourceConfig(context, ConfigMap.class,
                        interpolationResult -> interpolationResult.volumes().stream()
                                .flatMap(volume -> Optional.ofNullable(volume.getConfigMap())
                                        .map(ConfigMapVolumeSource::getName)
                                        .stream())),
                        context));
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
                    LOGGER.debug("Filter {} references {}(s) {}", ResourcesUtil.name(filter), secondaryClass.getName(), resourceIds);
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
                    LOGGER.debug("{} {} referenced by Filters {}", secondaryClass.getName(), ResourcesUtil.name(secret), resourceIds);
                    return resourceIds;
                })
                .build();
    }

    @Override
    public UpdateControl<KafkaProtocolFilter> reconcile(
                                                        KafkaProtocolFilter filter,
                                                        Context<KafkaProtocolFilter> context) {

        Map<String, Secret> existingSecretsByName = context.getSecondaryResourcesAsStream(Secret.class).collect(toByNameMap());
        LOGGER.debug("Existing secrets: {}", existingSecretsByName.keySet());

        Map<String, ConfigMap> existingConfigMapsByName = context.getSecondaryResourcesAsStream(ConfigMap.class).collect(toByNameMap());
        LOGGER.debug("Existing configmaps: {}", existingConfigMapsByName.keySet());

        var interpolationResult = secureConfigInterpolator.interpolate(filter.getSpec().getConfigTemplate());
        var referencedSecrets = interpolationResult.volumes().stream()
                .flatMap(volume -> Optional.ofNullable(volume.getSecret())
                        .map(SecretVolumeSource::getSecretName)
                        .stream())
                .collect(Collectors.toCollection(TreeSet::new));
        LOGGER.debug("Referenced secrets: {}", referencedSecrets);

        var referencedConfigMaps = interpolationResult.volumes().stream()
                .flatMap(volume -> Optional.ofNullable(volume.getConfigMap())
                        .map(ConfigMapVolumeSource::getName)
                        .stream())
                .collect(Collectors.toCollection(TreeSet::new));
        LOGGER.debug("Referenced configmaps: {}", referencedConfigMaps);

        KafkaProtocolFilter patch;
        if (existingSecretsByName.keySet().containsAll(referencedSecrets)
                && existingConfigMapsByName.keySet().containsAll(referencedConfigMaps)) {
            Stream<HasMetadata> referents = Stream.concat(referencedSecrets.stream().map(existingSecretsByName::get),
                    referencedConfigMaps.stream().map(existingConfigMapsByName::get));
            String checksum = MetadataChecksumGenerator.checksumFor(referents.toList());
            patch = statusFactory.newTrueConditionStatusPatch(
                    filter,
                    Condition.Type.ResolvedRefs,
                    checksum);
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
            LOGGER.info("Completed reconciliation of {}/{}", namespace(filter), name(filter));
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
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(filter), name(filter), e.toString());
        }
        return uc;
    }
}
