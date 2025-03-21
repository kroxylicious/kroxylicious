/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Reconciles a {@link KafkaProtocolFilter} by checking whether the {@link Secret}s
 * and {@link ConfigMap}s refered to in interpolated expressions actually exist, setting a
 * {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.
 */
public class KafkaProtocolFilterReconciler implements
        Reconciler<KafkaProtocolFilter> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProtocolFilterReconciler.class);
    private final Clock clock;
    private final SecureConfigInterpolator secureConfigInterpolator;

    KafkaProtocolFilterReconciler(Clock clock, SecureConfigInterpolator secureConfigInterpolator) {
        this.clock = clock;
        this.secureConfigInterpolator = secureConfigInterpolator;
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
     * @return The event source configuration
     * @param <R>
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
                    Set<String> resourceNames = resourceNameExtractor.apply(interpolationResult)
                            .collect(Collectors.toSet());
                    LOGGER.debug("Filter {} references {}(s) {}", ResourcesUtil.name(filter), secondaryClass.getName(), resourceNames);
                    return resourceNames.stream()
                            .map(name -> new ResourceID(name, ResourcesUtil.namespace(filter)))
                            .collect(Collectors.toSet());
                })
                .withSecondaryToPrimaryMapper(secret -> {
                    Set<ResourceID> resourceIDS = ResourcesUtil.filteredResourceIdsInSameNamespace(
                            context,
                            secret,
                            KafkaProtocolFilter.class,
                            filter -> {
                                Object configTemplate = filter.getSpec().getConfigTemplate();
                                var interpolationResult = secureConfigInterpolator.interpolate(configTemplate);
                                return resourceNameExtractor.apply(interpolationResult)
                                        .anyMatch(secretNameFromVolume -> secretNameFromVolume.equals(ResourcesUtil.name(secret)));
                            });
                    LOGGER.debug("{} {} referenced by Filters {}", secondaryClass.getName(), ResourcesUtil.name(secret), resourceIDS);
                    return resourceIDS;
                })
                .build();
    }

    @Override
    public UpdateControl<KafkaProtocolFilter> reconcile(
                                                        KafkaProtocolFilter filter,
                                                        Context<KafkaProtocolFilter> context) {

        var now = ZonedDateTime.ofInstant(clock.instant(), ZoneId.of("Z"));

        ConditionBuilder conditionBuilder = newResolvedRefsCondition(filter, now);

        var extentSecrets = context.getSecondaryResourcesAsStream(Secret.class)
                .map(ResourcesUtil::name)
                .collect(Collectors.toSet());
        LOGGER.debug("Extent secrets: {}", extentSecrets);

        var extentConfigMaps = context.getSecondaryResourcesAsStream(ConfigMap.class)
                .map(ResourcesUtil::name)
                .collect(Collectors.toSet());
        LOGGER.debug("Extent configmaps: {}", extentConfigMaps);

        var interpolationResult = secureConfigInterpolator.interpolate(filter.getSpec().getConfigTemplate());
        var referencedSecrets = interpolationResult.volumes().stream().flatMap(volume -> Optional.ofNullable(volume.getSecret())
                .map(SecretVolumeSource::getSecretName)
                .stream())
                .collect(Collectors.toCollection(HashSet::new));
        LOGGER.debug("Referenced secrets: {}", referencedSecrets);

        var referencedConfigMaps = interpolationResult.volumes().stream().flatMap(volume -> Optional.ofNullable(volume.getConfigMap())
                .map(ConfigMapVolumeSource::getName)
                .stream())
                .collect(Collectors.toCollection(HashSet::new));
        LOGGER.debug("Referenced configmaps: {}", referencedConfigMaps);

        if (extentSecrets.containsAll(referencedSecrets)
                && extentConfigMaps.containsAll(referencedConfigMaps)) {
            conditionBuilder.withStatus(Condition.Status.TRUE);
        }
        else {
            referencedSecrets.removeAll(extentSecrets);
            referencedConfigMaps.removeAll(extentConfigMaps);
            String message = "Referenced";
            if (!referencedSecrets.isEmpty()) {
                message += " Secrets " + referencedSecrets;
            }
            if (!referencedConfigMaps.isEmpty()) {
                message += " ConfigMaps " + referencedConfigMaps;
            }
            message += " not found";
            conditionBuilder.withStatus(Condition.Status.FALSE)
                    .withReason("MissingInterpolationReferences")
                    .withMessage(message);
        }

        KafkaProtocolFilter newFilter = newFilterWithCondition(filter, conditionBuilder.build());
        LOGGER.debug("Patching with status {}", newFilter.getStatus());
        return UpdateControl.patchStatus(newFilter);
    }

    @NonNull
    private static KafkaProtocolFilter newFilterWithCondition(KafkaProtocolFilter filter, Condition condition) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder(filter)
                    .withNewStatus()
                        .withObservedGeneration(ResourcesUtil.generation(filter))
                        .withConditions(condition) // overwrite any existing conditions
                    .endStatus()
                .build();
        // @formatter:on
    }

    private static ConditionBuilder newResolvedRefsCondition(KafkaProtocolFilter filter, ZonedDateTime now) {
        return new ConditionBuilder()
                .withType(Condition.Type.ResolvedRefs)
                .withLastTransitionTime(now)
                .withObservedGeneration(ResourcesUtil.generation(filter));
    }

    @Override
    public ErrorStatusUpdateControl<KafkaProtocolFilter> updateErrorStatus(
                                                                           KafkaProtocolFilter filter,
                                                                           Context<KafkaProtocolFilter> context,
                                                                           Exception e) {
        var now = ZonedDateTime.ofInstant(clock.instant(), ZoneId.of("Z"));
        // ResolvedRefs to UNKNOWN
        Condition condition = newResolvedRefsCondition(filter, now)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();
        return ErrorStatusUpdateControl.patchStatus(newFilterWithCondition(filter, condition));
    }
}
